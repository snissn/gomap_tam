#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"
#include "utils/syscache.h"

#include <dlfcn.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>

/* iceoryx2 C bindings */
#include "iox2/iceoryx2.h"

#include "treedb_pgext.h"

PG_MODULE_MAGIC;

/* Forward declarations */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void treedb_bgworker_main(Datum main_arg);

/* Path to the Go shared library, set at compile time via -DTDB_SHIM_PATH=... */
#ifndef TDB_SHIM_PATH
#error "TDB_SHIM_PATH must be defined at compile time (path to treedb_shim.so)"
#endif

/* iceoryx2 service names — must match treedb_pgext.h */
#define TDB_SERVICE_NAME       "treedb/bgworker"
#define TDB_EVENT_SERVICE_NAME "treedb/bgworker/event"

typedef int32_t (*treedb_init_fn)(const char *db_path);
typedef uint8_t (*treedb_handle_fn)(uint8_t opcode,
                                    const uint8_t *req, uint32_t req_len,
                                    uint8_t *resp_buf, uint32_t resp_buf_size,
                                    uint32_t *resp_len_out);

static volatile sig_atomic_t tdb_got_sigterm = 0;

int  treedb_transport_mode = TDB_TRANSPORT_ICEORYX;
bool treedb_pg_shmem_enabled = false;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static const struct config_enum_entry treedb_transport_options[] = {
    {"iceoryx",  TDB_TRANSPORT_ICEORYX,  false},
    {"pg_shmem", TDB_TRANSPORT_PG_SHMEM, false},
    {"pg-shmem", TDB_TRANSPORT_PG_SHMEM, true},
    {NULL, 0, false}
};

#define TDB_PG_SHMEM_NAME            "treedb_pgext pg-shmem rpc"
#define TDB_PG_SHMEM_LWLOCK_TRANCHE  "treedb_pgext_pg_shmem"
#define TDB_PG_SHMEM_MAGIC           0x54444253U /* TDBS */
#define TDB_PG_SHMEM_VERSION         1U
#define TDB_PG_SHMEM_SLOT_COUNT      64U
#define TDB_PG_SHMEM_DRAIN_BATCH     32U       /* Fairness: check iceoryx between batches. */
#define TDB_PG_SHMEM_WORKER_RETRIES  100       /* 100 x 100 ms = 10 s */
#define TDB_PG_SHMEM_WAIT_MS         1000L

#define TDB_PG_SLOT_FREE        0U
#define TDB_PG_SLOT_IDLE        1U
#define TDB_PG_SLOT_READY       2U
#define TDB_PG_SLOT_PROCESSING  3U
#define TDB_PG_SLOT_DONE        4U

typedef struct TDBPgShmemSlot
{
    uint32  state;
    uint32  generation;
    int     owner_pid;
    Latch  *owner_latch;

    uint8   opcode;
    uint32  req_len;
    uint8   req[TDB_MAX_REQ_PAYLOAD];

    uint8   status;
    uint32  resp_len;
    uint8   resp[TDB_MAX_RESP_PAYLOAD];
} TDBPgShmemSlot;

typedef struct TDBPgShmemState
{
    uint32  magic;
    uint32  version;
    uint32  slot_count;
    uint32  req_capacity;
    uint32  resp_capacity;

    uint32  next_generation;
    int     worker_pid;
    Latch  *worker_latch;
    bool    worker_ready;

    TDBPgShmemSlot slots[TDB_PG_SHMEM_SLOT_COUNT];
} TDBPgShmemState;

static TDBPgShmemState *tdb_pg_shmem_state = NULL;
static LWLockPadded    *tdb_pg_shmem_lwlocks = NULL;
static int              tdb_pg_shmem_slot_index = -1;
static uint32           tdb_pg_shmem_slot_generation = 0;
static bool             tdb_pg_shmem_exit_registered = false;

static void
tdb_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    tdb_got_sigterm = 1;
    if (MyLatch)
        SetLatch(MyLatch);
    errno = save_errno;
}

static Size
tdb_pg_shmem_size(void)
{
    return MAXALIGN(sizeof(TDBPgShmemState));
}

static LWLock *
tdb_pg_shmem_lock(void)
{
    if (tdb_pg_shmem_lwlocks == NULL)
        tdb_pg_shmem_lwlocks = GetNamedLWLockTranche(TDB_PG_SHMEM_LWLOCK_TRANCHE);
    return &tdb_pg_shmem_lwlocks[0].lock;
}

static bool
tdb_pid_alive(int pid)
{
    if (pid <= 0)
        return false;
    if (kill(pid, 0) == 0)
        return true;
    return errno != ESRCH;
}

static void
tdb_pg_shmem_reset_slot_locked(TDBPgShmemSlot *slot)
{
    slot->state       = TDB_PG_SLOT_FREE;
    slot->owner_pid   = 0;
    slot->owner_latch = NULL;
    slot->opcode      = 0;
    slot->req_len     = 0;
    slot->status      = TDB_STATUS_ERROR;
    slot->resp_len    = 0;
}

static void
tdb_pg_shmem_set_error_locked(TDBPgShmemSlot *slot, const char *msg)
{
    uint32 msglen = (uint32) strlen(msg);

    if (msglen > TDB_MAX_RESP_PAYLOAD)
        msglen = TDB_MAX_RESP_PAYLOAD;
    if (msglen > 0)
        memcpy(slot->resp, msg, msglen);
    slot->status   = TDB_STATUS_ERROR;
    slot->resp_len = msglen;
    slot->state    = TDB_PG_SLOT_DONE;
}

static void
tdb_pg_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    if (!treedb_pg_shmem_enabled)
        return;

    RequestAddinShmemSpace(tdb_pg_shmem_size());
    RequestNamedLWLockTranche(TDB_PG_SHMEM_LWLOCK_TRANCHE, 1);
}

static void
tdb_pg_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    if (!treedb_pg_shmem_enabled)
        return;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    tdb_pg_shmem_state = ShmemInitStruct(TDB_PG_SHMEM_NAME,
                                         tdb_pg_shmem_size(), &found);
    tdb_pg_shmem_lwlocks = GetNamedLWLockTranche(TDB_PG_SHMEM_LWLOCK_TRANCHE);

    if (!found)
    {
        memset(tdb_pg_shmem_state, 0, sizeof(TDBPgShmemState));
        tdb_pg_shmem_state->magic         = TDB_PG_SHMEM_MAGIC;
        tdb_pg_shmem_state->version       = TDB_PG_SHMEM_VERSION;
        tdb_pg_shmem_state->slot_count    = TDB_PG_SHMEM_SLOT_COUNT;
        tdb_pg_shmem_state->req_capacity  = TDB_MAX_REQ_PAYLOAD;
        tdb_pg_shmem_state->resp_capacity = TDB_MAX_RESP_PAYLOAD;
        tdb_pg_shmem_state->next_generation = 1;
        for (uint32 i = 0; i < TDB_PG_SHMEM_SLOT_COUNT; i++)
            tdb_pg_shmem_reset_slot_locked(&tdb_pg_shmem_state->slots[i]);
    }
    LWLockRelease(AddinShmemInitLock);
}

static void
tdb_pg_shmem_backend_exit(int code, Datum arg)
{
    LWLock         *lock;
    TDBPgShmemSlot *slot;

    (void) code;
    (void) arg;

    if (tdb_pg_shmem_state == NULL || tdb_pg_shmem_slot_index < 0)
        return;

    lock = tdb_pg_shmem_lock();
    LWLockAcquire(lock, LW_EXCLUSIVE);
    slot = &tdb_pg_shmem_state->slots[tdb_pg_shmem_slot_index];
    if (slot->owner_pid == MyProcPid &&
        slot->generation == tdb_pg_shmem_slot_generation)
    {
        if (slot->state == TDB_PG_SLOT_PROCESSING)
            slot->owner_latch = NULL; /* worker will finish; next backend can reclaim */
        else
            tdb_pg_shmem_reset_slot_locked(slot);
    }
    LWLockRelease(lock);

    tdb_pg_shmem_slot_index = -1;
    tdb_pg_shmem_slot_generation = 0;
}

static void
tdb_pg_shmem_error_cleanup(int code, Datum arg)
{
    int             slot_index = DatumGetInt32(arg);
    LWLock         *lock;
    TDBPgShmemSlot *slot;

    (void) code;

    if (tdb_pg_shmem_state == NULL || slot_index < 0 ||
        slot_index >= (int) TDB_PG_SHMEM_SLOT_COUNT)
        return;

    lock = tdb_pg_shmem_lock();
    LWLockAcquire(lock, LW_EXCLUSIVE);
    slot = &tdb_pg_shmem_state->slots[slot_index];
    if (slot->owner_pid == MyProcPid &&
        slot->generation == tdb_pg_shmem_slot_generation &&
        slot->state != TDB_PG_SLOT_PROCESSING)
    {
        slot->state    = TDB_PG_SLOT_IDLE;
        slot->req_len  = 0;
        slot->resp_len = 0;
    }
    LWLockRelease(lock);
}

static void
tdb_pg_shmem_worker_exit(int code, Datum arg)
{
    LWLock *lock;

    (void) code;
    (void) arg;

    if (!treedb_pg_shmem_enabled || tdb_pg_shmem_state == NULL)
        return;

    lock = tdb_pg_shmem_lock();
    LWLockAcquire(lock, LW_EXCLUSIVE);
    if (tdb_pg_shmem_state->worker_pid == MyProcPid)
    {
        for (uint32 i = 0; i < tdb_pg_shmem_state->slot_count; i++)
        {
            TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[i];
            if (slot->state == TDB_PG_SLOT_READY ||
                slot->state == TDB_PG_SLOT_PROCESSING)
            {
                tdb_pg_shmem_set_error_locked(slot, "background worker stopped");
                if (slot->owner_latch)
                    SetLatch(slot->owner_latch);
            }
        }
        tdb_pg_shmem_state->worker_ready = false;
        tdb_pg_shmem_state->worker_pid = 0;
        tdb_pg_shmem_state->worker_latch = NULL;
    }
    LWLockRelease(lock);
}

static void
tdb_pg_shmem_worker_ready(void)
{
    LWLock *lock;

    if (!treedb_pg_shmem_enabled)
        return;
    if (tdb_pg_shmem_state == NULL)
        ereport(ERROR,
                (errmsg("treedb: pg_shmem transport enabled but shared memory is not initialized")));

    lock = tdb_pg_shmem_lock();
    LWLockAcquire(lock, LW_EXCLUSIVE);
    tdb_pg_shmem_state->worker_pid = MyProcPid;
    tdb_pg_shmem_state->worker_latch = MyLatch;
    tdb_pg_shmem_state->worker_ready = true;
    LWLockRelease(lock);

    on_shmem_exit(tdb_pg_shmem_worker_exit, (Datum) 0);
}

static void
tdb_pg_shmem_check_ready(void)
{
    if (!treedb_pg_shmem_enabled)
        ereport(ERROR,
                (errmsg("treedb: pg_shmem transport requested but treedb.pg_shmem_enabled is off")));
    if (tdb_pg_shmem_state == NULL)
        ereport(ERROR,
                (errmsg("treedb: pg_shmem transport requested but shared memory is not initialized")));
    if (tdb_pg_shmem_state->magic != TDB_PG_SHMEM_MAGIC ||
        tdb_pg_shmem_state->version != TDB_PG_SHMEM_VERSION ||
        tdb_pg_shmem_state->req_capacity != TDB_MAX_REQ_PAYLOAD ||
        tdb_pg_shmem_state->resp_capacity != TDB_MAX_RESP_PAYLOAD)
        ereport(ERROR,
                (errmsg("treedb: pg_shmem transport shared-memory layout mismatch")));
}

static int
tdb_pg_shmem_ensure_slot(void)
{
    LWLock *lock;

    tdb_pg_shmem_check_ready();

    if (!tdb_pg_shmem_exit_registered)
    {
        on_proc_exit(tdb_pg_shmem_backend_exit, (Datum) 0);
        tdb_pg_shmem_exit_registered = true;
    }

    lock = tdb_pg_shmem_lock();

    if (tdb_pg_shmem_slot_index >= 0)
    {
        bool ok = false;

        LWLockAcquire(lock, LW_EXCLUSIVE);
        if (tdb_pg_shmem_slot_index < (int) tdb_pg_shmem_state->slot_count)
        {
            TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[tdb_pg_shmem_slot_index];
            if (slot->owner_pid == MyProcPid &&
                slot->generation == tdb_pg_shmem_slot_generation)
            {
                if (slot->state == TDB_PG_SLOT_IDLE ||
                    slot->state == TDB_PG_SLOT_DONE)
                {
                    slot->state = TDB_PG_SLOT_IDLE;
                    slot->req_len = 0;
                    slot->resp_len = 0;
                    ok = true;
                }
            }
        }
        LWLockRelease(lock);

        if (ok)
            return tdb_pg_shmem_slot_index;

        tdb_pg_shmem_slot_index = -1;
        tdb_pg_shmem_slot_generation = 0;
    }

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Reclaim slots from normally-exited backends.  Deep generation/restart
     * semantics are intentionally left to #4. */
    for (uint32 i = 0; i < tdb_pg_shmem_state->slot_count; i++)
    {
        TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[i];
        if (slot->state != TDB_PG_SLOT_FREE &&
            slot->state != TDB_PG_SLOT_PROCESSING &&
            slot->owner_pid > 0 && !tdb_pid_alive(slot->owner_pid))
            tdb_pg_shmem_reset_slot_locked(slot);
    }

    for (uint32 i = 0; i < tdb_pg_shmem_state->slot_count; i++)
    {
        TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[i];
        if (slot->state == TDB_PG_SLOT_FREE)
        {
            slot->state       = TDB_PG_SLOT_IDLE;
            slot->owner_pid   = MyProcPid;
            slot->owner_latch = MyLatch;
            slot->generation  = tdb_pg_shmem_state->next_generation++;
            slot->req_len     = 0;
            slot->resp_len    = 0;
            slot->status      = TDB_STATUS_ERROR;

            tdb_pg_shmem_slot_index = (int) i;
            tdb_pg_shmem_slot_generation = slot->generation;
            LWLockRelease(lock);
            return tdb_pg_shmem_slot_index;
        }
    }

    LWLockRelease(lock);
    ereport(ERROR,
            (errmsg("treedb: no free pg_shmem RPC slots (max %u)",
                    tdb_pg_shmem_state->slot_count)));
    return -1;
}

static void
tdb_pg_shmem_wait_worker_ready(void)
{
    LWLock *lock = tdb_pg_shmem_lock();

    for (int retry = 0; retry < TDB_PG_SHMEM_WORKER_RETRIES; retry++)
    {
        bool ready;
        int  pid;

        LWLockAcquire(lock, LW_SHARED);
        ready = tdb_pg_shmem_state != NULL &&
                tdb_pg_shmem_state->worker_ready &&
                tdb_pg_shmem_state->worker_latch != NULL;
        pid = tdb_pg_shmem_state != NULL ? tdb_pg_shmem_state->worker_pid : 0;
        LWLockRelease(lock);

        if (ready && tdb_pid_alive(pid))
            return;
        pg_usleep(100000L);
    }

    ereport(ERROR,
            (errmsg("treedb: pg_shmem transport has no ready background worker")));
}

static int
tdb_pg_shmem_submit(uint8 opcode, const void *req, uint32 req_len)
{
    int             slot_index;
    LWLock         *lock;
    TDBPgShmemSlot *slot;
    Latch          *worker_latch;
    int             worker_pid;
    bool            worker_ready;

    tdb_pg_shmem_check_ready();
    if (req_len > TDB_MAX_REQ_PAYLOAD)
        ereport(ERROR,
                (errmsg("treedb: pg_shmem RPC request too large (%u > %u)",
                        req_len, (uint32) TDB_MAX_REQ_PAYLOAD)));

    tdb_pg_shmem_wait_worker_ready();
    slot_index = tdb_pg_shmem_ensure_slot();
    lock = tdb_pg_shmem_lock();

    LWLockAcquire(lock, LW_EXCLUSIVE);
    slot = &tdb_pg_shmem_state->slots[slot_index];
    if (slot->owner_pid != MyProcPid ||
        slot->generation != tdb_pg_shmem_slot_generation ||
        slot->state != TDB_PG_SLOT_IDLE)
    {
        LWLockRelease(lock);
        ereport(ERROR, (errmsg("treedb: pg_shmem backend slot is not reusable")));
    }

    slot->opcode      = opcode;
    slot->req_len     = req_len;
    slot->status      = TDB_STATUS_ERROR;
    slot->resp_len    = 0;
    slot->owner_latch = MyLatch;
    if (req_len > 0)
        memcpy(slot->req, req, req_len);

    worker_ready = tdb_pg_shmem_state->worker_ready;
    worker_pid   = tdb_pg_shmem_state->worker_pid;
    worker_latch = tdb_pg_shmem_state->worker_latch;
    slot->state  = TDB_PG_SLOT_READY;
    LWLockRelease(lock);

    if (!worker_ready || worker_latch == NULL || !tdb_pid_alive(worker_pid))
    {
        LWLockAcquire(lock, LW_EXCLUSIVE);
        if (slot->owner_pid == MyProcPid &&
            slot->generation == tdb_pg_shmem_slot_generation &&
            slot->state == TDB_PG_SLOT_READY)
            slot->state = TDB_PG_SLOT_IDLE;
        LWLockRelease(lock);
        ereport(ERROR,
                (errmsg("treedb: pg_shmem background worker is not available")));
    }

    SetLatch(worker_latch);
    return slot_index;
}

static void
tdb_pg_shmem_wait_done(int slot_index)
{
    LWLock *lock = tdb_pg_shmem_lock();

    ResetLatch(MyLatch);
    for (;;)
    {
        int  worker_pid;
        bool worker_ready;
        bool done = false;

        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(lock, LW_SHARED);
        if (slot_index >= 0 && slot_index < (int) tdb_pg_shmem_state->slot_count)
        {
            TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[slot_index];
            done = slot->owner_pid == MyProcPid &&
                   slot->generation == tdb_pg_shmem_slot_generation &&
                   slot->state == TDB_PG_SLOT_DONE;
        }
        worker_ready = tdb_pg_shmem_state->worker_ready;
        worker_pid = tdb_pg_shmem_state->worker_pid;
        LWLockRelease(lock);

        if (done)
            return;

        if (!worker_ready || !tdb_pid_alive(worker_pid))
        {
            LWLockAcquire(lock, LW_EXCLUSIVE);
            if (slot_index >= 0 && slot_index < (int) tdb_pg_shmem_state->slot_count)
            {
                TDBPgShmemSlot *slot = &tdb_pg_shmem_state->slots[slot_index];
                if (slot->owner_pid == MyProcPid &&
                    slot->generation == tdb_pg_shmem_slot_generation &&
                    slot->state != TDB_PG_SLOT_DONE)
                    slot->state = TDB_PG_SLOT_IDLE;
            }
            LWLockRelease(lock);
            ereport(ERROR,
                    (errmsg("treedb: pg_shmem background worker exited before responding")));
        }

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         TDB_PG_SHMEM_WAIT_MS, 0);
        ResetLatch(MyLatch);
    }
}

static uint8
tdb_pg_shmem_finish_palloc(int slot_index,
                           void **resp_out, uint32 *resp_len_out)
{
    LWLock         *lock = tdb_pg_shmem_lock();
    TDBPgShmemSlot *slot;
    uint8           status;
    uint32          rlen;
    void           *buf = NULL;
    char            errmsg_buf[256] = "unknown error from background worker";

    LWLockAcquire(lock, LW_SHARED);
    slot = &tdb_pg_shmem_state->slots[slot_index];
    if (slot->owner_pid != MyProcPid ||
        slot->generation != tdb_pg_shmem_slot_generation ||
        slot->state != TDB_PG_SLOT_DONE)
    {
        LWLockRelease(lock);
        ereport(ERROR, (errmsg("treedb: pg_shmem response slot is invalid")));
    }
    status = slot->status;
    rlen = slot->resp_len;
    if (rlen > TDB_MAX_RESP_PAYLOAD)
        rlen = TDB_MAX_RESP_PAYLOAD;
    if (status == TDB_STATUS_ERROR && rlen > 0)
    {
        uint32 msglen = rlen < sizeof(errmsg_buf) - 1
                        ? rlen : (uint32)(sizeof(errmsg_buf) - 1);
        memcpy(errmsg_buf, slot->resp, msglen);
        errmsg_buf[msglen] = '\0';
    }
    LWLockRelease(lock);

    if (status != TDB_STATUS_ERROR && rlen > 0 && resp_out != NULL)
        buf = palloc(rlen);

    LWLockAcquire(lock, LW_EXCLUSIVE);
    slot = &tdb_pg_shmem_state->slots[slot_index];
    if (slot->owner_pid == MyProcPid &&
        slot->generation == tdb_pg_shmem_slot_generation &&
        slot->state == TDB_PG_SLOT_DONE)
    {
        if (status != TDB_STATUS_ERROR && rlen > 0 && buf != NULL)
            memcpy(buf, slot->resp, rlen);
        slot->state = TDB_PG_SLOT_IDLE;
        slot->req_len = 0;
        slot->resp_len = 0;
    }
    LWLockRelease(lock);

    if (status == TDB_STATUS_ERROR)
        ereport(ERROR, (errmsg("treedb: %s", errmsg_buf)));

    if (buf != NULL && resp_out != NULL)
    {
        *resp_out = buf;
        if (resp_len_out) *resp_len_out = rlen;
    }
    else
    {
        if (resp_out)     *resp_out = NULL;
        if (resp_len_out) *resp_len_out = 0;
    }

    return status;
}

static uint8
tdb_pg_shmem_finish_into(int slot_index,
                         void *resp_buf, uint32 resp_buf_size,
                         uint32 *resp_len_out)
{
    LWLock         *lock = tdb_pg_shmem_lock();
    TDBPgShmemSlot *slot;
    uint8           status;
    uint32          rlen;
    char            errmsg_buf[256] = "unknown error from background worker";

    LWLockAcquire(lock, LW_EXCLUSIVE);
    slot = &tdb_pg_shmem_state->slots[slot_index];
    if (slot->owner_pid != MyProcPid ||
        slot->generation != tdb_pg_shmem_slot_generation ||
        slot->state != TDB_PG_SLOT_DONE)
    {
        LWLockRelease(lock);
        ereport(ERROR, (errmsg("treedb: pg_shmem response slot is invalid")));
    }

    status = slot->status;
    rlen = slot->resp_len;
    if (rlen > TDB_MAX_RESP_PAYLOAD)
        rlen = TDB_MAX_RESP_PAYLOAD;

    if (status == TDB_STATUS_ERROR)
    {
        if (rlen > 0)
        {
            uint32 msglen = rlen < sizeof(errmsg_buf) - 1
                            ? rlen : (uint32)(sizeof(errmsg_buf) - 1);
            memcpy(errmsg_buf, slot->resp, msglen);
            errmsg_buf[msglen] = '\0';
        }
    }
    else if (rlen > 0)
    {
        if (resp_buf == NULL || resp_buf_size < rlen)
        {
            slot->state = TDB_PG_SLOT_IDLE;
            slot->req_len = 0;
            slot->resp_len = 0;
            LWLockRelease(lock);
            ereport(ERROR,
                    (errmsg("treedb: pg_shmem RPC response too large for caller buffer (%u > %u)",
                            rlen, resp_buf_size)));
        }
        memcpy(resp_buf, slot->resp, rlen);
    }

    slot->state = TDB_PG_SLOT_IDLE;
    slot->req_len = 0;
    slot->resp_len = 0;
    LWLockRelease(lock);

    if (status == TDB_STATUS_ERROR)
        ereport(ERROR, (errmsg("treedb: %s", errmsg_buf)));

    if (resp_len_out)
        *resp_len_out = (rlen > 0 && status != TDB_STATUS_ERROR) ? rlen : 0;
    return status;
}

uint8
tdb_pg_shmem_rpc(uint8 opcode,
                  const void *req, uint32 req_len,
                  void **resp_out, uint32 *resp_len_out)
{
    uint8 status;
    int   slot_index = tdb_pg_shmem_submit(opcode, req, req_len);

    PG_ENSURE_ERROR_CLEANUP(tdb_pg_shmem_error_cleanup, Int32GetDatum(slot_index));
    {
        tdb_pg_shmem_wait_done(slot_index);
        status = tdb_pg_shmem_finish_palloc(slot_index, resp_out, resp_len_out);
    }
    PG_END_ENSURE_ERROR_CLEANUP(tdb_pg_shmem_error_cleanup, Int32GetDatum(slot_index));

    return status;
}

uint8
tdb_pg_shmem_rpc_into(uint8 opcode,
                       const void *req, uint32 req_len,
                       void *resp_buf, uint32 resp_buf_size,
                       uint32 *resp_len_out)
{
    uint8 status;
    int   slot_index = tdb_pg_shmem_submit(opcode, req, req_len);

    PG_ENSURE_ERROR_CLEANUP(tdb_pg_shmem_error_cleanup, Int32GetDatum(slot_index));
    {
        tdb_pg_shmem_wait_done(slot_index);
        status = tdb_pg_shmem_finish_into(slot_index,
                                          resp_buf, resp_buf_size,
                                          resp_len_out);
    }
    PG_END_ENSURE_ERROR_CLEANUP(tdb_pg_shmem_error_cleanup, Int32GetDatum(slot_index));

    return status;
}

/* Context threaded through request draining helpers. */
typedef struct
{
    iox2_server_h    *server;
    treedb_handle_fn  handle_fn;
    uint8_t          *resp_buf;
    uint32_t          resp_buf_size;
} tdb_bgworker_ctx_t;

/*
 * tdb_process_requests_cb — WaitSet callback.
 *
 * Called by iox2_waitset_wait_and_process_once_with_timeout whenever the
 * listener fires (i.e. a backend sent a notify after writing its request).
 * Drains all pending requests in a tight loop, dispatches each to the Go
 * shim, and sends the response.  Returns CONTINUE so the WaitSet keeps
 * running; the outer while-loop is responsible for checking SIGTERM.
 */
static iox2_callback_progression_e
tdb_process_requests_cb(iox2_waitset_attachment_id_h attachment_id,
                        iox2_callback_context ctx)
{
    tdb_bgworker_ctx_t   *wctx;
    iox2_active_request_h active_req;
    iox2_response_mut_h   response;
    const uint8_t        *req_data;
    c_size_t              req_elems;
    uint8_t              *resp_payload;
    uint8_t               opcode;
    const uint8_t        *payload;
    uint32_t              payload_len;
    uint8_t               status;
    uint32_t              resp_len;
    c_size_t              total_resp;
    int                   ret;

    (void) attachment_id; /* single attachment — no need to check which */

    wctx = (tdb_bgworker_ctx_t *) ctx;

    while (true)
    {
        active_req = NULL;
        ret = iox2_server_receive(wctx->server, NULL, &active_req);
        if (ret != IOX2_OK || active_req == NULL)
            break;

        req_data  = NULL;
        req_elems = 0;
        iox2_active_request_payload(&active_req,
                                    (const void **) &req_data, &req_elems);

        if (req_elems < 1)
        {
            iox2_active_request_drop(active_req);
            continue;
        }

        opcode      = req_data[0];
        payload     = req_data + 1;
        payload_len = (uint32_t)(req_elems - 1);

        resp_len = 0;
        status = wctx->handle_fn(opcode, payload, payload_len,
                                  wctx->resp_buf, wctx->resp_buf_size, &resp_len);
        if (resp_len > wctx->resp_buf_size)
        {
            const char *msg = "response exceeds transport buffer capacity";
            status = TDB_STATUS_ERROR;
            resp_len = (uint32_t) strlen(msg);
            if (resp_len > wctx->resp_buf_size)
                resp_len = wctx->resp_buf_size;
            if (resp_len > 0)
                memcpy(wctx->resp_buf, msg, resp_len);
        }

        total_resp   = (c_size_t)(1 + resp_len);
        response     = NULL;
        resp_payload = NULL;
        ret = iox2_active_request_loan_slice_uninit(
                &active_req, NULL, &response, total_resp);
        if (ret != IOX2_OK)
        {
            ereport(WARNING,
                    (errmsg("treedb: loan response slice failed: %d", ret)));
            iox2_active_request_drop(active_req);
            continue;
        }

        iox2_response_mut_payload_mut(&response, (void **) &resp_payload, NULL);
        resp_payload[0] = status;
        if (resp_len > 0)
            memcpy(resp_payload + 1, wctx->resp_buf, resp_len);

        iox2_response_mut_send(response);
        iox2_active_request_drop(active_req);
    }

    return iox2_callback_progression_e_CONTINUE;
}

static void
tdb_iox2_event_noop_cb(const iox2_event_id_t *event_id,
                       uint64_t number_of_events,
                       iox2_callback_context ctx)
{
    (void) event_id;
    (void) number_of_events;
    (void) ctx;
}

static void
tdb_iox2_drain_nonblocking(iox2_listener_h *listener,
                           tdb_bgworker_ctx_t *wctx)
{
    uint64_t n_notifs = 0;
    int      ret;

    ret = iox2_listener_try_wait(listener, &n_notifs,
                                 tdb_iox2_event_noop_cb, NULL);
    if (ret != IOX2_OK)
        ereport(WARNING,
                (errmsg("treedb: listener wait error %d; continuing", ret)));

    (void) tdb_process_requests_cb(NULL, wctx);
}

static uint32
tdb_pg_shmem_drain(treedb_handle_fn handle_fn,
                   uint8_t *resp_buf, uint32_t resp_buf_size,
                   uint32 max_requests)
{
    LWLock *lock;
    uint32  processed = 0;
    uint8_t req_buf[TDB_MAX_REQ_PAYLOAD];

    if (!treedb_pg_shmem_enabled || tdb_pg_shmem_state == NULL || max_requests == 0)
        return 0;

    lock = tdb_pg_shmem_lock();

    for (;;)
    {
        TDBPgShmemSlot *slot = NULL;
        int             slot_index = -1;
        uint32          generation = 0;
        int             owner_pid = 0;
        uint8           opcode = 0;
        uint32          req_len = 0;
        uint8           status;
        uint32          resp_len = 0;
        Latch          *owner_latch = NULL;

        LWLockAcquire(lock, LW_EXCLUSIVE);

        for (uint32 i = 0; i < tdb_pg_shmem_state->slot_count; i++)
        {
            slot = &tdb_pg_shmem_state->slots[i];

            if (slot->state != TDB_PG_SLOT_FREE &&
                slot->state != TDB_PG_SLOT_PROCESSING &&
                slot->owner_pid > 0 && !tdb_pid_alive(slot->owner_pid))
            {
                tdb_pg_shmem_reset_slot_locked(slot);
                continue;
            }

            if (slot->state == TDB_PG_SLOT_READY)
            {
                slot_index = (int) i;
                generation = slot->generation;
                owner_pid = slot->owner_pid;
                opcode = slot->opcode;
                req_len = slot->req_len;
                break;
            }
        }

        if (slot_index < 0)
        {
            LWLockRelease(lock);
            break;
        }

        if (req_len > TDB_MAX_REQ_PAYLOAD)
        {
            tdb_pg_shmem_set_error_locked(slot, "pg_shmem request exceeds slot capacity");
            owner_latch = slot->owner_latch;
            LWLockRelease(lock);
            if (owner_latch)
                SetLatch(owner_latch);
            processed++;
            if (processed >= max_requests)
                break;
            continue;
        }

        if (req_len > 0)
            memcpy(req_buf, slot->req, req_len);
        slot->state = TDB_PG_SLOT_PROCESSING;
        LWLockRelease(lock);

        status = handle_fn(opcode, req_buf, req_len,
                           resp_buf, resp_buf_size, &resp_len);
        if (resp_len > resp_buf_size)
        {
            status = TDB_STATUS_ERROR;
            resp_len = (uint32) strlen("pg_shmem response exceeds slot capacity");
            memcpy(resp_buf, "pg_shmem response exceeds slot capacity", resp_len);
        }

        LWLockAcquire(lock, LW_EXCLUSIVE);
        slot = &tdb_pg_shmem_state->slots[slot_index];
        if (slot->generation == generation &&
            slot->owner_pid == owner_pid &&
            slot->state == TDB_PG_SLOT_PROCESSING)
        {
            slot->status = status;
            slot->resp_len = resp_len;
            if (resp_len > 0)
                memcpy(slot->resp, resp_buf, resp_len);
            slot->state = TDB_PG_SLOT_DONE;
            owner_latch = slot->owner_latch;
        }
        LWLockRelease(lock);

        if (owner_latch)
            SetLatch(owner_latch);
        processed++;
        if (processed >= max_requests)
            break;
    }

    return processed;
}

/* ----------------------------------------------------------------
 * DROP TABLE cleanup
 *
 * PostgreSQL has no TAM callback for DROP TABLE, so we hook into
 * object_access_hook to detect when a treedb table is dropped and
 * record its relfilenode.  On transaction commit we send a TRUNCATE
 * RPC to delete all TreeDB data for that relfilenode; on abort we
 * discard the list (the table wasn't actually dropped).
 * ---------------------------------------------------------------- */

static object_access_hook_type prev_object_access_hook = NULL;
static List *treedb_pending_drops = NIL;

static void
treedb_xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT)
    {
        ListCell *lc;
        foreach(lc, treedb_pending_drops)
            tdb_truncate_rpc((RelFileNumber) lfirst_int(lc));
    }

    if (event == XACT_EVENT_COMMIT   || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT)
    {
        list_free(treedb_pending_drops);
        treedb_pending_drops = NIL;
    }
}

static void
treedb_object_access(ObjectAccessType access, Oid classId, Oid objectId,
                     int subId, void *arg)
{
    HeapTuple       tuple;
    Form_pg_class   relform;
    Oid             treedb_am;
    RelFileNumber   relnum;

    /* Chain to any previously registered hook. */
    if (prev_object_access_hook)
        prev_object_access_hook(access, classId, objectId, subId, arg);

    /* Only care about DROP on pg_class entries (i.e. relations). */
    if (access != OAT_DROP || classId != RelationRelationId)
        return;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objectId));
    if (!HeapTupleIsValid(tuple))
        return;

    relform = (Form_pg_class) GETSTRUCT(tuple);

    if (relform->relkind != RELKIND_RELATION)
    {
        ReleaseSysCache(tuple);
        return;
    }

    treedb_am = get_table_am_oid("treedb", true);
    if (!OidIsValid(treedb_am) || relform->relam != treedb_am)
    {
        ReleaseSysCache(tuple);
        return;
    }

    relnum = relform->relfilenode;
    ReleaseSysCache(tuple);

    /* Defer the actual cleanup until commit so aborted DROPs don't wipe data. */
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);
        treedb_pending_drops = lappend_int(treedb_pending_drops, (int) relnum);
        MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * _PG_init: called when the extension is loaded.
 * Registers the background worker and DROP TABLE cleanup hooks.
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("treedb_pgext must be loaded via shared_preload_libraries")));

    DefineCustomBoolVariable("treedb.pg_shmem_enabled",
                             "Allocate PostgreSQL shared memory for TreeDB RPC.",
                             "When on at postmaster start, the opt-in treedb.transport=pg_shmem path is available.",
                             &treedb_pg_shmem_enabled,
                             false,
                             PGC_POSTMASTER,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomEnumVariable("treedb.transport",
                             "TreeDB TAM RPC transport.",
                             "iceoryx is the default. pg_shmem is accepted only when treedb.pg_shmem_enabled is on.",
                             &treedb_transport_mode,
                             TDB_TRANSPORT_ICEORYX,
                             treedb_transport_options,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    MarkGUCPrefixReserved("treedb");

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = tdb_pg_shmem_request;
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = tdb_pg_shmem_startup;

    /* Register DROP TABLE cleanup hooks (fire in backend processes). */
    prev_object_access_hook = object_access_hook;
    object_access_hook      = treedb_object_access;
    RegisterXactCallback(treedb_xact_callback, NULL);

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time   = BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = 5; /* restart after 5s on crash */
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "treedb_pgext");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "treedb_bgworker_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "treedb background worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "treedb");
    worker.bgw_main_arg     = (Datum) 0;
    worker.bgw_notify_pid   = 0;

    RegisterBackgroundWorker(&worker);
}

/*
 * treedb_bgworker_main: entry point for the background worker process.
 *
 * Architecture:
 *   1. dlopen Go shim, call treedb_init() to open/create the DB directory.
 *   2. Create an iceoryx2 node + request/response service as the SERVER.
 *   3. Loop: receive requests from backends via iceoryx2, dispatch to
 *      treedb_handle(), send response back.
 */
void
treedb_bgworker_main(Datum main_arg)
{
    void               *shim_handle;
    treedb_init_fn      init_fn;
    treedb_handle_fn    handle_fn;
    char                db_path[MAXPGPATH];

    /* iceoryx2 request/response handles */
    iox2_node_builder_h                      node_builder = NULL;
    iox2_node_h                              node_handle  = NULL;
    iox2_service_name_h                      svc_name     = NULL;
    iox2_service_builder_h                   svc_builder  = NULL;
    iox2_service_builder_request_response_h  sb_rr;
    iox2_port_factory_request_response_h     service      = NULL;
    iox2_port_factory_server_builder_h       srv_builder  = NULL;
    iox2_server_h                            server       = NULL;

    /* iceoryx2 event service handles (notifier + WaitSet wake-up) */
    iox2_service_name_h                      evt_svc_name = NULL;
    iox2_service_builder_h                   evt_svc_bldr = NULL;
    iox2_service_builder_event_h             evt_sb;
    iox2_port_factory_event_h                evt_factory  = NULL;
    iox2_port_factory_listener_builder_h     lst_builder  = NULL;
    iox2_listener_h                          listener     = NULL;
    iox2_waitset_builder_h                   ws_builder   = NULL;
    iox2_waitset_h                           waitset      = NULL;
    iox2_waitset_guard_h                     guard        = NULL;
    iox2_file_descriptor_ptr                 listener_fd;

    tdb_bgworker_ctx_t       wctx;
    iox2_waitset_run_result_e ws_result;

    /* Response scratch buffer. Large enough for a full SCAN_NEXT_BATCH batch. */
    uint8_t  resp_buf[TDB_MAX_RESP_PAYLOAD];

    int ret;

    pqsignal(SIGTERM, tdb_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    tdb_db_path(db_path, sizeof(db_path));

    if (mkdir(db_path, 0700) < 0 && errno != EEXIST)
        ereport(ERROR,
                (errmsg("treedb: could not create data directory \"%s\": %m",
                        db_path)));

    /* --- Load Go shim --- */
    shim_handle = dlopen(TDB_SHIM_PATH, RTLD_NOW | RTLD_LOCAL);
    if (!shim_handle)
        ereport(ERROR,
                (errmsg("treedb: dlopen(\"%s\") failed: %s",
                        TDB_SHIM_PATH, dlerror())));

    init_fn = (treedb_init_fn) dlsym(shim_handle, "treedb_init");
    if (!init_fn)
        ereport(ERROR,
                (errmsg("treedb: dlsym(treedb_init) failed: %s", dlerror())));

    handle_fn = (treedb_handle_fn) dlsym(shim_handle, "treedb_handle");
    if (!handle_fn)
        ereport(ERROR,
                (errmsg("treedb: dlsym(treedb_handle) failed: %s", dlerror())));

    if (init_fn(db_path) != 0)
        ereport(ERROR,
                (errmsg("treedb: treedb_init(\"%s\") failed", db_path)));

    /* --- Set up iceoryx2 node --- */
    iox2_set_log_level_from_env_or(iox2_log_level_e_WARN);

    node_builder = iox2_node_builder_new(NULL);
    ret = iox2_node_builder_create(node_builder, NULL,
                                   iox2_service_type_e_IPC, &node_handle);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: iox2_node_builder_create failed: %d", ret)));

    /* --- Set up event service (listener + WaitSet) ---
     *
     * Created BEFORE the request/response service so that by the time a
     * client successfully opens the rr service, the event service already
     * exists and the client can open it without retrying.
     */
    ret = iox2_service_name_new(NULL, TDB_EVENT_SERVICE_NAME,
                                strlen(TDB_EVENT_SERVICE_NAME), &evt_svc_name);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: event service_name_new failed: %d", ret)));

    evt_svc_bldr = iox2_node_service_builder(&node_handle, NULL,
                                             iox2_cast_service_name_ptr(evt_svc_name));
    evt_sb = iox2_service_builder_event(evt_svc_bldr);
    /* Allow up to 64 concurrent backend notifiers; only 1 listener (us). */
    iox2_service_builder_event_set_max_notifiers(&evt_sb, 64);
    iox2_service_builder_event_set_max_listeners(&evt_sb, 1);
    ret = iox2_service_builder_event_open_or_create(evt_sb, NULL, &evt_factory);
    if (ret != IOX2_OK)
    {
        int first_ret = ret;

        /* If an older/stale client-created service exists with smaller limits,
         * open it without our preferred capacity hints instead of killing the
         * singleton worker.  The response-wait timeout still fails clients
         * closed if no live server can answer. */
        evt_svc_bldr = iox2_node_service_builder(&node_handle, NULL,
                                                 iox2_cast_service_name_ptr(evt_svc_name));
        evt_sb = iox2_service_builder_event(evt_svc_bldr);
        ret = iox2_service_builder_event_open_or_create(evt_sb, NULL, &evt_factory);
        if (ret != IOX2_OK)
            ereport(ERROR,
                    (errmsg("treedb: event service open_or_create failed: %d (retry %d)",
                            first_ret, ret)));
    }

    lst_builder = iox2_port_factory_event_listener_builder(&evt_factory, NULL);
    ret = iox2_port_factory_listener_builder_create(lst_builder, NULL, &listener);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: listener create failed: %d", ret)));

    listener_fd = iox2_listener_get_file_descriptor(&listener);

    iox2_waitset_builder_new(NULL, &ws_builder);
    /* Disable iceoryx2 signal handling — we manage SIGTERM ourselves. */
    iox2_waitset_builder_set_signal_handling_mode(&ws_builder,
                                                  iox2_signal_handling_mode_e_DISABLED);
    ret = iox2_waitset_builder_create(ws_builder, iox2_service_type_e_IPC,
                                      NULL, &waitset);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: waitset create failed: %d", ret)));

    ret = iox2_waitset_attach_notification(&waitset, listener_fd, NULL, &guard);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: waitset attach_notification failed: %d", ret)));

    iox2_port_factory_event_drop(evt_factory);
    iox2_service_name_drop(evt_svc_name);

    /* --- Set up request/response service --- */
    ret = iox2_service_name_new(NULL, TDB_SERVICE_NAME,
                                strlen(TDB_SERVICE_NAME), &svc_name);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: iox2_service_name_new failed: %d", ret)));

    svc_builder = iox2_node_service_builder(&node_handle, NULL,
                                            iox2_cast_service_name_ptr(svc_name));
    sb_rr = iox2_service_builder_request_response(svc_builder);

    ret = iox2_service_builder_request_response_set_request_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: set request type details failed: %d", ret)));

    ret = iox2_service_builder_request_response_set_response_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: set response type details failed: %d", ret)));

    ret = iox2_service_builder_request_response_open_or_create(sb_rr, NULL, &service);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: open_or_create service failed: %d", ret)));

    srv_builder = iox2_port_factory_request_response_server_builder(&service, NULL);
    iox2_port_factory_server_builder_set_initial_max_slice_len(
            &srv_builder, (c_size_t) TDB_MAX_RESP_SLICE);

    ret = iox2_port_factory_server_builder_create(srv_builder, NULL, &server);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: server create failed: %d", ret)));

    iox2_service_name_drop(svc_name);
    iox2_port_factory_request_response_drop(service);

    if (treedb_pg_shmem_enabled)
        tdb_pg_shmem_worker_ready();

    ereport(LOG,
            (errmsg("treedb background worker ready: db=%s service=%s pg_shmem=%s",
                    db_path, TDB_SERVICE_NAME,
                    treedb_pg_shmem_enabled ? "on" : "off")));

    /* --- Main event loop ---
     *
     * Default iceoryx-only mode uses the existing iceoryx2 WaitSet.  When the
     * opt-in PostgreSQL shared-memory transport is enabled, use PostgreSQL's
     * latch/socket wait so backend SetLatch() calls wake the worker promptly,
     * while still draining iceoryx notifications for sessions left on the
     * default/fallback transport.
     */
    wctx.server        = &server;
    wctx.handle_fn     = handle_fn;
    wctx.resp_buf      = resp_buf;
    wctx.resp_buf_size = sizeof(resp_buf);

    if (treedb_pg_shmem_enabled)
    {
        pgsocket iox2_sock = (pgsocket) iox2_file_descriptor_native_handle(listener_fd);

        if (iox2_sock == PGINVALID_SOCKET)
            ereport(ERROR, (errmsg("treedb: iceoryx listener has no native file descriptor")));

        while (!tdb_got_sigterm)
        {
            int    rc;
            uint32 shmem_processed;

            /* Fairness: pg_shmem is opt-in but iceoryx remains the default.
             * Drain only a bounded shared-memory batch before giving iceoryx a
             * nonblocking service chance, so steady pg_shmem traffic cannot
             * starve default/fallback iceoryx sessions until client timeout. */
            shmem_processed = tdb_pg_shmem_drain(handle_fn, resp_buf,
                                                 sizeof(resp_buf),
                                                 TDB_PG_SHMEM_DRAIN_BATCH);
            if (shmem_processed > 0)
            {
                tdb_iox2_drain_nonblocking(&listener, &wctx);
                if (shmem_processed >= TDB_PG_SHMEM_DRAIN_BATCH)
                    continue;
            }

            rc = WaitLatchOrSocket(MyLatch,
                                   WL_LATCH_SET | WL_SOCKET_READABLE |
                                   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                                   iox2_sock, 1000L, 0);

            if (rc & WL_LATCH_SET)
            {
                ResetLatch(MyLatch);
                shmem_processed = tdb_pg_shmem_drain(handle_fn, resp_buf,
                                                     sizeof(resp_buf),
                                                     TDB_PG_SHMEM_DRAIN_BATCH);
                if (shmem_processed > 0)
                    tdb_iox2_drain_nonblocking(&listener, &wctx);
            }

            if (rc & WL_SOCKET_READABLE)
                tdb_iox2_drain_nonblocking(&listener, &wctx);
        }
    }
    else
    {
        while (!tdb_got_sigterm)
        {
            ws_result = (iox2_waitset_run_result_e) 0;
            ret = iox2_waitset_wait_and_process_once_with_timeout(
                    &waitset, tdb_process_requests_cb, &wctx,
                    1 /* seconds */, 0 /* nanoseconds */, &ws_result);
            if (ret != IOX2_OK)
                ereport(WARNING,
                        (errmsg("treedb: waitset error %d (result %d); continuing",
                                ret, (int) ws_result)));
        }
    }

    /* Cleanup */
    iox2_waitset_guard_drop(guard);
    iox2_waitset_drop(waitset);
    iox2_listener_drop(listener);
    iox2_server_drop(server);
    iox2_node_drop(node_handle);
    dlclose(shim_handle);
    proc_exit(0);
}
