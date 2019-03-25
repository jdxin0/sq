#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#include <zmq.h>
#include <msgpack.h>
#include <leveldb/c.h>

struct sq_header {
    char _action[16],
    char _queue[128],
    unsigned long _mid,
    int _priority,
    int _dlp,
    int _redeliver,
    int _ack,
    int _timeout
};
static const ACTIONS_COUNT = 8;
static const char ACTIONS[][] = {"push", "pop", "ack", "nack", "drop", "conf", "stat", "namespace"};
static const char WORKERS_INPROC[] = "inproc://workers";

static sq_header sq_header_from_msgpack (char * data, size_t size, msgpack_zone mempool) {
    sq_header h = {
            ._action="",
            ._queue="",
            ._priority=0,
            ._dlp=0,
            ._mid=0,
            ._redeliver=0,
            ._ack=1,
            ._timeout=60
    }

    msgpack_object deserialized;
    msgpack_unpack (data, size, NULL, &mempool, &deserialized);

    if (deserialized.type == MSGPACK_OBJECT_MAP) {
        msgpack_object_print (stdout, deserialized);
        printf ("\n");
        msgpack_object_kv *ptr = deserialized.via.map.ptr;

        char key[] = ""

        for (int i = 0; i < deserialized.via.map.size; i++) {
            ptr += i;
            msgpack_object key = ptr->key;
            msgpack_object value = ptr->val;

            if ( key.type == MSGPACK_OBJECT_STR) {
                for (int i = 0; i < key.via.str.size; i++) {
                    printf ("%c", key.via.str.ptr[i]);
                }
            }
            printf (" ");
            if ( value.type == MSGPACK_OBJECT_STR) {
                for (int i = 0; i < value.via.str.size; i++) {
                    printf ("%c", value.via.str.ptr[i]);
                }
            }
            printf ("\n");
        }

        for (int i = 0; i < ACTIONS_COUNT; i++) {
            if (ACTIONS[i] == h._action) {
                return h
            }
        }
    }

    return NULL
}


static void * worker_routine (void *context) {
    void *socket = zmq_socket (context, ZMQ_REP);
    int rc = zmq_connect (socket, WORKERS_INPROC);
    assert (rc == 0);

    msgpack_zone mempool;
    int result = msgpack_zone_init (&mempool, 1024);
    assert (result == 1);

    while (1) {
        zmq_msg_t header;
        zmq_msg_t body;

        rc = zmq_msg_init (&header);
        assert (rc == 0);
        rc = zmq_msg_init (&body);
        assert (rc == 0);

        rc = zmq_msg_recv (&header, socket, 0);
        assert (rc != -1);

        size_t size = zmq_msg_size (&header);

        char *data = (char *) zmq_msg_data (&header);

        msgpack_object deserialized;
        msgpack_unpack (data, size, NULL, &mempool, &deserialized);

        if ( deserialized.type == MSGPACK_OBJECT_MAP) {
            msgpack_object_print (stdout, deserialized);
            printf ("\n");
            msgpack_object_kv *ptr = deserialized.via.map.ptr;

            for (int i = 0; i < deserialized.via.map.size; i++) {
                ptr += i;
                msgpack_object key = ptr->key;
                msgpack_object value = ptr->val;

                if ( key.type == MSGPACK_OBJECT_STR) {
                    for (int i = 0; i < key.via.str.size; i++) {
                        printf ("%c", key.via.str.ptr[i]);
                    }
                }
                printf (" ");
                if ( value.type == MSGPACK_OBJECT_STR) {
                    for (int i = 0; i < value.via.str.size; i++) {
                        printf ("%c", value.via.str.ptr[i]);
                    }
                }
                printf ("\n");
            }

            if (zmq_msg_more (&header)) {
                rc = zmq_msg_recv (&body, socket, 0);
                assert (rc != -1);
                rc = zmq_msg_send (&header, socket, ZMQ_SNDMORE);
                assert (rc != -1);
                rc = zmq_msg_send (&body, socket, 0);
                assert (rc != -1);
            } else {
                rc = zmq_msg_send (&header, socket, 0);
                assert (rc != -1);
            }

        } else {
            msgpack_sbuffer sbuf;
            msgpack_sbuffer_init(&sbuf);
            msgpack_packer pk;
            msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
            msgpack_pack_map (&pk, 1);

            rc = zmq_send (socket, sbuf.data, sbuf.size);
            assert (rc == sbuf.size);

            msgpack_sbuffer_destroy(&sbuf);
        }


        zmq_msg_close (&header);
        zmq_msg_close (&body);
    }

    msgpack_zone_destroy (&mempool);

    zmq_close (socket);
    return NULL;
}


int main (void) {
    const int threads_num_io = 1;
    const int threads_num_worker = 1;
    const char GATEWAY_TCP[] = "tcp://*:5555";

    void *context = zmq_ctx_new ();
    zmq_ctx_set (context, ZMQ_IO_THREADS, threads_num_io);

    void *gateway = zmq_socket (context, ZMQ_ROUTER);
    zmq_bind (gateway, GATEWAY_TCP);

    void *workers = zmq_socket (context, ZMQ_DEALER);
    zmq_bind (workers, WORKERS_INPROC);

    for (int thread_nbr = 0; thread_nbr < threads_num_worker; thread_nbr++) {
        pthread_t worker;
        pthread_create (&worker, NULL, worker_routine, context);
    }

    zmq_proxy (gateway, workers, NULL);

    zmq_close (gateway);
    zmq_close (workers);
    zmq_ctx_term (context);
    return 0;
}
