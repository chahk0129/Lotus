#include "system/global.h"
#include "system/manager.h"
#include "system/memory_region.h"
#include "transport/transport.h"
#include "transport/message.h"
#include "batch/table.h"
#include "utils/helper.h"
#include "utils/debug.h"
#include <fcntl.h>
#include <cstring>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/socket.h>
#include <fstream>

#define PRINT_DEBUG_INFO false

// The socket code is borrowed from
// http://easy-tutorials.net/c/linux-c-socket-programming/
// The external program execution code is borrowed from http://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c-using-posix
transport_t::transport_t() {
    read_urls();
}

transport_t::~transport_t() {
    cleanup();
}

bool transport_t::init() {
    char hostname[1024];
    memset(hostname, '\0', 1024);
    gethostname(hostname, 1023);
    uint32_t global_node_id = -1;
    // Determine the global node ID based on the hostname
    // and the URLs read from the configuration file.
    if (g_is_server) { 
        for(uint32_t i=0; i<g_num_server_nodes; i++) {
            if (_server_urls[i] == std::string(hostname)) {
                global_node_id = i;
                break;
            }
        }
    }
    else {
        assert(g_is_server == false);
        for(uint32_t i=0; i<g_num_client_nodes; i++) {
            if (_client_urls[i] == std::string(hostname)) {
                global_node_id = i;
                break;
            }
        }
    }

    assert(global_node_id != -1);
    g_node_id = global_node_id;
    printf("NodeID: %u, \tHostname: %s\n", g_node_id, hostname);
    if(g_is_server) g_num_nodes = g_num_client_nodes;
    else g_num_nodes = g_num_server_nodes;
    global_manager->set_node_id(g_node_id);

    // initialize communication buffers and structures
    _sockets = new int[g_num_nodes];

    _send_buffer = new char*[g_num_nodes];
    _recv_buffer = new char*[g_num_nodes];
    _recv_buffer_lower = new uint32_t[g_num_nodes];
    _recv_buffer_upper = new uint32_t[g_num_nodes];

    for(uint32_t i=0; i<g_num_nodes; i++) {
        _send_buffer[i] = new char[MAX_MESSAGE_SIZE];
        _recv_buffer[i] = new char[MAX_MESSAGE_SIZE];
        _recv_buffer_lower[i] = 0;
        _recv_buffer_upper[i] = 0;
    }
    return true;
}

void transport_t::server_listen() {
    struct addrinfo info;
    memset(&info, 0, sizeof(info));
    for (uint32_t i=0; i<g_num_nodes; i++) {
        info.ai_family = AF_UNSPEC;    // Allow IPv4 or IPv6
        info.ai_socktype = SOCK_STREAM; // SOCK_STREAM for TCP or SOCK_DGRAM for UDP
        info.ai_flags = AI_PASSIVE;

        struct addrinfo* host_info_list;
        uint32_t port_num = get_port_num(i, g_is_server);
        printf("Binding to port %u\n", port_num);
        int status = getaddrinfo(nullptr, std::to_string(port_num).c_str(), &info, &host_info_list);
        assert(status == 0);

        // create socket
        int sockfd = socket(host_info_list->ai_family, host_info_list->ai_socktype, host_info_list->ai_protocol);
        uint32_t buffer_size = MAX_MESSAGE_SIZE;
        setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(uint32_t));

        int flag = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));

        _sockets[i] = sockfd;
        assert(_sockets[i] != -1);

        // bind socket
        int op = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &op, sizeof(int));
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &op, sizeof(int));
        status = bind(sockfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
        if (status == -1) {
            int bind_errno = errno;
            printf("ERROR: bind() failed for port %s\n", std::to_string(get_port_num(i, g_is_server)).c_str());
            printf("Error: %s (errno: %d)\n", strerror(bind_errno), bind_errno);
            printf("Socket: %d, Node: %u, Is Server: %d\n", sockfd, i, g_is_server);
            
            // Check what's using this port
            std::string cmd = "netstat -tlnp | grep :" + std::to_string(get_port_num(i, g_is_server));
            printf("Checking port usage: %s\n", cmd.c_str());
            system(cmd.c_str());
        }
        assert(status != -1);
        status = listen(sockfd, 5);
        assert(status != -1);
        fcntl(sockfd, F_SETFL, O_NONBLOCK); // Set socket to non-blocking mode
    }

    // Accept connection from client nodes
    for(uint32_t i=0; i<g_num_nodes; i++) {
        int new_fd = -1;
        struct sockaddr_storage their_addr;
        socklen_t addr_size = sizeof(their_addr);
        int sockfd = _sockets[i];
        do {
            new_fd = accept(sockfd, (struct sockaddr*)&their_addr, &addr_size);
        } while(new_fd == -1);
        _sockets[i] = new_fd;
        fcntl(new_fd, F_SETFL, O_NONBLOCK); // Set socket to non-blocking mode
    }
}

void transport_t::client_connect() {
    struct addrinfo info;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        memset(&info, 0, sizeof(info));
        info.ai_family = AF_UNSPEC;    // Allow IPv4 or IPv6
        info.ai_socktype = SOCK_STREAM; // SOCK_STREAM for TCP or SOCK_DGRAM for UDP
        info.ai_flags = AI_PASSIVE;

        struct addrinfo* host_info_list = nullptr;
        std::string node_name = _server_urls[i];
        uint32_t port_num = get_port_num(i, g_is_server);
        printf("Connecting to node %u at %s:%u\n", i, node_name.c_str(), port_num);
        // std::string port_num = std::to_string(get_port_num(g_node_id, !g_is_server));
        int status;
        do {
            status = getaddrinfo(node_name.c_str(), std::to_string(port_num).c_str(), &info, &host_info_list);
        } while(status != 0);

        // create socket
        int sockfd;
        do {
            sockfd = socket(host_info_list->ai_family, host_info_list->ai_socktype, host_info_list->ai_protocol);
        } while(sockfd == -1);
        _sockets[i] = sockfd;

        do {
            status = ::connect(sockfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
            PAUSE
        } while(status == -1);

        uint32_t buffer_size = MAX_MESSAGE_SIZE;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(uint32_t));
        int flag = 1;
        int res = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        assert(res != -1);

        setsockopt(sockfd, SOL_SOCKET, TCP_QUICKACK, &flag, sizeof(int));
    }
}

bool transport_t::connect() {
    // Create lock socket for each node
    memset(_sockets, 0, sizeof(int) * g_num_nodes);
    if (g_is_server) server_listen();
    else client_connect();
    printf("Remote sockets initialized\n");
    return true;
}

void transport_t::close_sockets() {
    if (_sockets) {
        for(uint32_t i=0; i<g_num_nodes; i++) {
            if (_sockets[i] > 0) {
                shutdown(_sockets[i], SHUT_RDWR);
                ::close(_sockets[i]);
                _sockets[i] = -1;
            }
        }
        delete[] _sockets;
        _sockets = nullptr;
    }

    if (_send_buffer) {
        for(uint32_t i=0; i<g_num_nodes; i++) {
            if (_send_buffer[i]) {
                delete[] _send_buffer[i];
                _send_buffer[i] = nullptr;
            }
        }
        delete[] _send_buffer;
        _send_buffer = nullptr;
    }

    if (_recv_buffer) {
        for(uint32_t i=0; i<g_num_nodes; i++) {
            if (_recv_buffer[i]) {
                delete[] _recv_buffer[i];
                _recv_buffer[i] = nullptr;
            }
        }
        delete[] _recv_buffer;
        _recv_buffer = nullptr;
    }

    if (_recv_buffer_lower) {
        delete[] _recv_buffer_lower;
        _recv_buffer_lower = nullptr;
    }

    if (_recv_buffer_upper) {
        delete[] _recv_buffer_upper;
        _recv_buffer_upper = nullptr;
    }
}

void transport_t::sendMsg(message_t* msg, uint32_t node_id) {
    uint32_t packet_size = msg->get_packet_len();
    char data[packet_size];
    msg->to_packet(data);
    uint32_t bytes_sent = 0;
    while (bytes_sent < packet_size) {
        int bytes = ::send(_sockets[node_id], data + bytes_sent, packet_size - bytes_sent, 0);
        if(bytes > 0)
            bytes_sent += bytes;
    }
    #if PRINT_DEBUG_INFO
    printf("\033[1;31m[QP_ID=%5ld] Send %d->%d %16s [%4d bytes] fd=%d \033[0m\n",
           msg->get_qp_id(), GLOBAL_NODE_ID, node_id, msg->get_name().c_str(), bytes_sent,
           _sockets[node_id]);
    #endif

}

message_t* transport_t::recvMsg() {
    ssize_t bytes = 0;
    uint32_t global_node_id = g_node_id;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        uint32_t node_id = i;

        if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] <= MAX_MESSAGE_SIZE) {
            // not enough bytes in the recv buffer
            if (MAX_MESSAGE_SIZE - _recv_buffer_upper[node_id] < MAX_MESSAGE_SIZE / 4) {
                // Buffer is almost full, shift it
                memmove(_recv_buffer[node_id], 
                        _recv_buffer[node_id] + _recv_buffer_lower[node_id], 
                        _recv_buffer_upper[node_id] - _recv_buffer_lower[node_id]);
                _recv_buffer_upper[node_id] -= _recv_buffer_lower[node_id];
                _recv_buffer_lower[node_id] = 0;
            }

            uint32_t max_size = MAX_MESSAGE_SIZE - _recv_buffer_upper[node_id];
            bytes = ::recv(_sockets[i], _recv_buffer[node_id] + _recv_buffer_upper[node_id], max_size, MSG_DONTWAIT);
            if (bytes > 0)
                _recv_buffer_upper[node_id] += bytes;
        }

        if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] >= sizeof(message_t)) {
            auto msg = (message_t*)(_recv_buffer[node_id] + _recv_buffer_lower[node_id]);
            assert(msg->get_packet_len() < MAX_MESSAGE_SIZE);
            if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] >= msg->get_packet_len()) {
                // Find a valid input message
                msg = new message_t(_recv_buffer[node_id] + _recv_buffer_lower[node_id]); 
                _recv_buffer_lower[node_id] += msg->get_packet_len();
                if (_recv_buffer_upper[node_id] == _recv_buffer_lower[node_id]) {
                    // a small optimization
                    _recv_buffer_lower[node_id] = 0;
                    _recv_buffer_upper[node_id] = 0;
                }
                #if PRINT_DEBUG_INFO
                printf("\033[1;32m[TxnID=%5ld] recv %d<-%d %16s [%4d bytes] fd=%d \033[0m\n",
                       msg->get_txn_id(), global_node_id, node_id, 
                       msg->get_name().c_str(), msg->get_packet_len(),
                       _sockets[node_id]);
                #endif
                return msg;
            }
        }
    }
    return nullptr;
}

void transport_t::read_urls() {
    // get server names
    std::string line = "";
    std::ifstream file(ifconfig_file);
    assert(file.is_open());

    uint32_t num_server_nodes = 0;
    uint32_t num_client_nodes = 0;
    uint32_t node_type = -1;
    while (std::getline(file, line)) {
        if (line[0] == '#')
            continue; // skip comments
        else if (line[0] == '=') {
            switch (line[1]) {
                case 'c' : node_type = 0; break; // client
                case 's' : node_type = 1; break; // server
                default: node_type = -1; break; // unknown
            }
        }
        else {
            switch (node_type) {
                case 0: { num_client_nodes++; _client_urls.push_back(line); break; }
                case 1: { num_server_nodes++; _server_urls.push_back(line); break; }
                default: { assert(false); }
            }
        }
    }

    assert(num_client_nodes > 0 && num_server_nodes > 0);
    g_num_client_nodes = num_client_nodes;
    g_num_server_nodes = num_server_nodes;

    file.close();
}

uint32_t transport_t::get_port_num(uint32_t node_id, bool is_server) {
    if (is_server) return START_PORT + node_id + g_node_id * g_num_nodes;
    else return START_PORT + g_node_id + node_id * g_num_nodes;
}

bool transport_t::init_rdma() {
    memset(&_context, 0, sizeof(struct rdma_ctx));

    _context.gid_idx = 0;
    _context.ctx = open_device();
    if (!_context.ctx) return cleanup();

    _context.pd = alloc_pd(_context.ctx);
    if (!_context.pd) return cleanup();

    bool ret = query_attr(_context.ctx, _context.port_attr, _context.gid_idx, _context.gid);
    if (!ret) return cleanup();

    // QPs and CQs are initialized in the derived classes
    // Clients and servers have different # of QPs and CQs

    // we allocate memory regions for each node (this MR is only for two-sided RDMA)
    //// buffer layout for server
    ////// [ send region * g_num_server_threads | recv region * g_num_client_threads * g_num_client_nodes ]
    //// buffer layout for client
    ////// [ send region * g_num_client_threads | recv region * g_num_server_threads * g_num_server_nodes ]
    uint64_t global_num_send_threads = g_num_client_threads < g_num_server_threads ? g_num_server_threads : g_num_client_threads;
    uint64_t global_num_recv_threads = g_num_client_threads;
    //uint64_t global_num_recv_threads = g_is_server ? g_num_client_threads : g_num_server_threads;
    _rdma_send_buffer_size = MAX_MESSAGE_SIZE * global_num_send_threads * g_num_nodes;
    _rdma_recv_buffer_size = MAX_MESSAGE_SIZE * global_num_recv_threads * g_num_nodes;

    #if BATCH
    g_num_batch_threads = g_num_client_threads / batch_table_t::MAX_GROUP_SIZE;
    if (g_num_client_threads % batch_table_t::MAX_GROUP_SIZE != 0) g_num_batch_threads++;
    _rdma_send_batch_buffer_size = MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * g_num_batch_threads * g_num_nodes;
    _rdma_recv_batch_buffer_size = MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * g_num_batch_threads * g_num_nodes;
    debug::notify_info("Batching enabled ... %u batch buffers", g_num_batch_threads);
    #else
    g_num_batch_threads = 0;
    _rdma_send_batch_buffer_size = 0;
    _rdma_recv_batch_buffer_size = 0;
    #endif

    uint64_t regular_size = _rdma_send_buffer_size + _rdma_recv_buffer_size;
    uint64_t batch_size = _rdma_send_batch_buffer_size + _rdma_recv_batch_buffer_size;
    uint64_t total_size = regular_size + batch_size;
    _memory_region = new memory_region_t(total_size);

    _rdma_send_buffer = new char**[global_num_send_threads];
    char* send_base = reinterpret_cast<char*>(_memory_region->ptr());
    uint32_t send_size_per_thread = _rdma_send_buffer_size / global_num_send_threads;
    for (uint32_t i=0; i<global_num_send_threads; i++) {
        _rdma_send_buffer[i] = new char*[g_num_nodes];
        char* send_base_i = send_base + i * send_size_per_thread;
        for (uint32_t j=0; j<g_num_nodes; j++)
            _rdma_send_buffer[i][j] = send_base_i + MAX_MESSAGE_SIZE * j;
    }   

    _rdma_recv_buffer = new char**[global_num_recv_threads];
    char* recv_base = reinterpret_cast<char*>(_memory_region->ptr()) + _rdma_send_buffer_size;
    uint32_t recv_size_per_thread = _rdma_recv_buffer_size / global_num_recv_threads;
    for (uint32_t i=0; i<global_num_recv_threads; i++) {
        _rdma_recv_buffer[i] = new char*[g_num_nodes];
        char* recv_base_i = recv_base + i * recv_size_per_thread;
        for (uint32_t j=0; j<g_num_nodes; j++)
            _rdma_recv_buffer[i][j] = recv_base_i + MAX_MESSAGE_SIZE * j;
    }

    #if BATCH
    _rdma_send_batch_buffer = new char**[g_num_batch_threads];
    char* send_batch_base = reinterpret_cast<char*>(_memory_region->ptr()) + regular_size;
    uint32_t send_batch_size_per_thread = _rdma_send_batch_buffer_size / g_num_batch_threads;
    for (uint32_t i=0; i<g_num_batch_threads; i++) {
        _rdma_send_batch_buffer[i] = new char*[g_num_nodes];
        char* send_batch_base_i = send_batch_base + i * send_batch_size_per_thread;
        for (uint32_t j=0; j<g_num_nodes; j++)
            _rdma_send_batch_buffer[i][j] = send_batch_base_i + MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * j;
    }

    _rdma_recv_batch_buffer = new char**[g_num_batch_threads];
    char* recv_batch_base = reinterpret_cast<char*>(_memory_region->ptr()) + regular_size + _rdma_send_batch_buffer_size;
    uint32_t recv_batch_size_per_thread = _rdma_recv_batch_buffer_size / g_num_batch_threads;
    for (uint32_t i=0; i<g_num_batch_threads; i++) {
        _rdma_recv_batch_buffer[i] = new char*[g_num_nodes];
        char* recv_batch_base_i = recv_batch_base + i * recv_batch_size_per_thread;
        for (uint32_t j=0; j<g_num_nodes; j++)
            _rdma_recv_batch_buffer[i][j] = recv_batch_base_i + MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * j;
    }
    #else
    _rdma_send_batch_buffer = nullptr;
    _rdma_recv_batch_buffer = nullptr;
    #endif

/*
    _rdma_send_buffer = new char**[g_num_nodes];
    char* send_base = reinterpret_cast<char*>(_memory_region->ptr());
    uint32_t send_size_per_node = _rdma_send_buffer_size / g_num_nodes;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        _rdma_send_buffer[i] = new char*[global_num_send_threads];
        char* send_base_i = send_base + i * send_size_per_node;
        for (uint32_t j=0; j<global_num_send_threads; j++)
            _rdma_send_buffer[i][j] = send_base_i + MAX_MESSAGE_SIZE * j;
    }

    _rdma_recv_buffer = new char**[g_num_nodes];
    char* recv_base = reinterpret_cast<char*>(_memory_region->ptr()) + _rdma_send_buffer_size;
    uint32_t recv_size_per_node = _rdma_recv_buffer_size / g_num_nodes;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        _rdma_recv_buffer[i] = new char*[global_num_recv_threads];
        char* recv_base_i = recv_base + i * recv_size_per_node;
        for (uint32_t j=0; j<global_num_recv_threads; j++)
            _rdma_recv_buffer[i][j] = recv_base_i + j * MAX_MESSAGE_SIZE;
    }

    #if BATCH
    _rdma_send_batch_buffer = new char**[g_num_nodes];
    char* send_batch_base = reinterpret_cast<char*>(_memory_region->ptr()) + regular_size;
    uint32_t send_batch_size_per_node = _rdma_send_batch_buffer_size / g_num_nodes;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        _rdma_send_batch_buffer[i] = new char*[g_num_batch_threads];
        char* send_batch_base_i = send_batch_base + i * send_batch_size_per_node;
        for (uint32_t j=0; j<g_num_batch_threads; j++)
            _rdma_send_batch_buffer[i][j] = send_batch_base_i + MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * j;
    }

    _rdma_recv_batch_buffer = new char**[g_num_nodes];
    char* recv_batch_base = reinterpret_cast<char*>(_memory_region->ptr()) + regular_size + _rdma_send_batch_buffer_size;
    uint32_t recv_batch_size_per_node = _rdma_recv_batch_buffer_size / g_num_nodes;
    for (uint32_t i=0; i<g_num_nodes; i++) {
        _rdma_recv_batch_buffer[i] = new char*[g_num_batch_threads];
        char* recv_batch_base_i = recv_batch_base + i * recv_batch_size_per_node;
        for (uint32_t j=0; j<g_num_batch_threads; j++)
            _rdma_recv_batch_buffer[i][j] = recv_batch_base_i + MAX_MESSAGE_SIZE * batch_table_t::MAX_GROUP_SIZE * j;
    }
    #endif
*/
    return true;
}

bool transport_t::cleanup() {
    if (_rdma_send_buffer) {
        for (uint32_t i=0; i<g_num_client_threads; i++) {
        // for (uint32_t i=0; i<g_num_nodes; i++) {
            if (_rdma_send_buffer[i]) {
                delete[] _rdma_send_buffer[i];
                _rdma_send_buffer[i] = nullptr;
            }
        }
        delete[] _rdma_send_buffer;
        _rdma_send_buffer = nullptr;
    }
    if (_rdma_recv_buffer) {
        for (uint32_t i=0; i<g_num_client_threads; i++) {
        // for (uint32_t i=0; i<g_num_nodes; i++) {
            if (_rdma_recv_buffer[i]) {
                delete[] _rdma_recv_buffer[i];
                _rdma_recv_buffer[i] = nullptr;
            }
        }
        delete[] _rdma_recv_buffer;
        _rdma_recv_buffer = nullptr;
    }
    if (_rdma_send_batch_buffer) {
        for (uint32_t i=0; i<g_num_batch_threads; i++) {
        // for (uint32_t i=0; i<g_num_nodes; i++) {
            if (_rdma_send_batch_buffer[i]) {
                delete[] _rdma_send_batch_buffer[i];
                _rdma_send_batch_buffer[i] = nullptr;
            }
        }
        delete[] _rdma_send_batch_buffer;
        _rdma_send_batch_buffer = nullptr;
    }
    if (_rdma_recv_batch_buffer) {
        for (uint32_t i=0; i<g_num_batch_threads; i++) {
        // for (uint32_t i=0; i<g_num_nodes; i++) {
            if (_rdma_recv_batch_buffer[i]) {
                delete[] _rdma_recv_batch_buffer[i];
                _rdma_recv_batch_buffer[i] = nullptr;
            }
        }
        delete[] _rdma_recv_batch_buffer;
        _rdma_recv_batch_buffer = nullptr;
    }
    if (_memory_region) {
        delete _memory_region;
        _memory_region = nullptr;
    }
    if (_context.pd) ibv_dealloc_pd(_context.pd);
    if (_context.ctx) ibv_close_device(_context.ctx);

    close_sockets();
    return false;
}

struct ibv_context* transport_t::open_device() {
    struct ibv_device** dev_list = nullptr;
    struct ibv_device* dev = nullptr;
    int flags = 0;
    int dev_num = 0;

    dev_list = ibv_get_device_list(&dev_num);
    if (!dev_list || !dev_num){
        debug::notify_error("Failed to ibv_get_device_list");
        return nullptr;
    }

    for (int i=0; i<dev_num; i++) {
        // if(ibv_get_device_name(dev_list[i])[5] == '0') { // open mlx5_0
        if (ibv_get_device_name(dev_list[i])[5] == '2') { // open mlx5_2
            dev = dev_list[i];
            debug::notify_info("Opening %s\n", ibv_get_device_name(dev_list[i]));
            break;
        }
    }

    if (!dev) {
        debug::notify_error("Failed to find IB device");
        return nullptr;
    }

    auto ctx = ibv_open_device(dev);
    if (!ctx) {
        debug::notify_error("Failed to ibv_open_device");
        return nullptr;
    }

    ibv_free_device_list(dev_list);

    // if (g_is_server) {
    //     struct ibv_query_device_ex_input input;
    //     memset(&device_attr, 0, sizeof(device_attr));
    //     device_attr.comp_mask = IBV_DEVICE_ATTR_UMR | IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE;
    //     if (ibv_query_device_ex(ctx, &input, &device_attr)) {
    //         debug::notify_error("Failed to ibv_query_device_ex");
    //         return ctx;
    //     }
    //     if (ibv_query_device(ctx, &device_attr)) {
    //         debug::notify_error("Failed to ibv_query_device");
    //         return ctx;
    //     }

    //     if (device_attr.max_mr_size == 0) {
    //         debug::notify_error("This RNIC does not support device memory");
    //         assert(false);
    //         return ctx;
    //     }

    //     debug::notify_info("Max Device memory = %lu KB\n", device_attr.max_mr_size / 1024);

        // struct ibv_exp_device_attr device_attr;
        // memset(&device_attr, 0, sizeof(device_attr));
        // device_attr.comp_mask = IBV_EXP_DEVICE_ATTR_UMR | IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE;
        // if (ibv_exp_query_device(ctx, &device_attr)) {
        //     debug::notify_error("Failed to ibv_exp_query_device");
        //     return ctx;
        // }

        // if (!(device_attr.comp_mask & IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE)
        //     || (device_attr.max_dm_size == 0)) {
        //     debug::notify_error("This RNIC does not support device memory");
        //     assert(false);
        //     return ctx;
        // }
        // debug::notify_info("Max Device memory = %lu KB\n", device_attr.max_dm_size / 1024);
    // }
    return ctx;
}

struct ibv_pd* transport_t::alloc_pd(struct ibv_context* ctx) {
    auto pd = ibv_alloc_pd(ctx);
    if (!pd) {
        debug::notify_error("Failed to ibv_alloc_pd");
        return nullptr;
    }
    return pd;
}

bool transport_t::query_attr(struct ibv_context* ctx, struct ibv_port_attr& attr, int& gid_idx, union ibv_gid& gid) {
    memset(&attr, 0, sizeof(attr));

    if (ibv_query_port(ctx, IB_PORT, &attr)) {
        debug::notify_error("Failed to ibv_query_port");
        return false;
    }

    if (ibv_query_gid(ctx, IB_PORT, gid_idx, &gid)) {
        debug::notify_error("Failed to ibv_query_gid");
        return false;
    }
    return true;
}

struct ibv_cq* transport_t::create_cq(struct ibv_context* ctx){
    auto cq = ibv_create_cq(ctx, QP_DEPTH, nullptr, nullptr, 0);
    if(!cq){
        debug::notify_error("Failed to ibv_create_cq");
        return nullptr;
    }
    return cq;
}

struct ibv_qp* transport_t::create_qp(struct ibv_pd* pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq){
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_type = IBV_QPT_RC;
    attr.send_cq = send_cq;
    attr.recv_cq = recv_cq;

    attr.cap.max_send_wr = QP_DEPTH;
    attr.cap.max_recv_wr = QP_DEPTH;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 512;

    auto qp = ibv_create_qp(pd, &attr);
    if (!qp) {
        debug::notify_error("Failed to ibv_create_qp");
        return nullptr;
    }

    return qp;
}

struct ibv_mr* transport_t::register_mr(struct ibv_pd* pd, void* addr, uint64_t size) {
    int flags = IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC;
    auto mr = ibv_reg_mr(pd, addr, size, flags);
    if (!mr) {
        debug::notify_error("Failed to ibv_reg_mr");
        assert(false);
        return nullptr;
    }
    return mr;
}

struct ibv_mr* transport_t::register_mr_device(struct ibv_context* ctx, struct ibv_pd* pd, void* addr, uint64_t size) {
    struct ibv_alloc_dm_attr dm_attr;
    memset(&dm_attr, 0, sizeof(dm_attr));
    dm_attr.length = size;
    struct ibv_dm* dm = ibv_alloc_dm(ctx, &dm_attr);
    if (!dm) {
        debug::notify_error("Failed to ibv_alloc_dm (addr %p, size %lu): err %s (errno %d)", addr, size, strerror(errno), errno);
        return nullptr;
    }
    int flags = IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC |
                IBV_ACCESS_ZERO_BASED;
    struct ibv_mr* mr = ibv_reg_dm_mr(pd, dm, 0, size, flags);
    if (!mr) {
        debug::notify_error("Failed to ibv_reg_dm_mr");
        return nullptr;
    }

    char* buffer = (char*)malloc(size);
    memset(buffer, 0, size);
    ibv_memcpy_to_dm(dm, 0, buffer, size);
    free(buffer);

    // struct ibv_exp_alloc_dm_attr dm_attr;
    // memset(&dm_attr, 0, sizeof(dm_attr));
    // dm_attr.length = size;
    // struct ibv_exp_dm* dm = ibv_exp_alloc_dm(ctx, &dm_attr);
    // if (!dm) {
    //     debug::notify_error("Failed to ibv_exp_alloc_dm");
    //     return nullptr;
    // }

    // struct ibv_exp_reg_mr_in mr_in;
    // memset(&mr_in, 0, sizeof(mr_in));
    // int flags = IBV_ACCESS_LOCAL_WRITE |
    //             IBV_ACCESS_REMOTE_READ |
    //             IBV_ACCESS_REMOTE_WRITE |
    //             IBV_ACCESS_REMOTE_ATOMIC;
    // mr_in.pd = pd;
    // mr_in.addr = (void*)addr;
    // mr_in.dm = dm;
    // mr_in.length = size;
    // // mr_in.create_flags = 0;
    // mr_in.exp_access = flags;
    // mr_in.comp_mask = IBV_EXP_REG_MR_DM;
    // struct ibv_mr* mr = ibv_exp_reg_mr(&mr_in);
    // if (!mr) {
    //     debug::notify_error("Failed to ibv_exp_reg_dm_mr");
    //     return nullptr;
    // }

    // // init the memory to zero
    // char* buffer = (char*)malloc(size);
    // memset(buffer, 0, size);
    // assert(buffer);

    // struct ibv_exp_memcpy_dm_attr memcpy_attr;
    // memset(&memcpy_attr, 0, sizeof(memcpy_attr));
    // memcpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
    // memcpy_attr.host_addr = (void*)buffer;
    // memcpy_attr.length = size;
    // memcpy_attr.dm_offset = 0;
    // ibv_exp_memcpy_dm(dm, &memcpy_attr);
    // free(buffer);
    // debug::notify_info("Device memory registered: %lu KB\n", size / 1024);

    return mr;
}

bool transport_t::modify_qp_state_to_init(struct ibv_qp* qp) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = IB_PORT;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_READ |
                          IBV_ACCESS_REMOTE_WRITE |
                          IBV_ACCESS_REMOTE_ATOMIC;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    if (ibv_modify_qp(qp, &attr, flags)) {
        debug::notify_error("Failed to modify QP state to INIT");
        return false;
    }
    return true;
}

bool transport_t::modify_qp_state_to_rtr(struct ibv_qp* qp, union ibv_gid dest_gid, int gid_idx, uint32_t lid, uint32_t qpn) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = qpn;
    attr.rq_psn = 3185;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;

    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = IB_PORT;
    memcpy(&attr.ah_attr.grh.dgid, &dest_gid, sizeof(dest_gid));
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = gid_idx;
    attr.ah_attr.grh.traffic_class = 0;

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    if (ibv_modify_qp(qp, &attr, flags)) {
        debug::notify_error("Failed to modify QP state to RTR");
        return false;
    }
    return true;
}

bool transport_t::modify_qp_state_to_rts(struct ibv_qp* qp) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7; // infinite retry
    attr.sq_psn = 3185;
    attr.max_rd_atomic = 16;

    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    if (ibv_modify_qp(qp, &attr, flags)) {
        debug::notify_error("Failed to modify QP state to RTS");
        return false;
    }
    return true;
}

char* transport_t::get_buffer(){
    return _rdma_send_buffer[GET_THD_ID][0];
    // return _rdma_send_buffer[0][GET_THD_ID];
}

char* transport_t::get_buffer(uint32_t node_id) {
    return _rdma_send_buffer[GET_THD_ID][node_id];
    // return _rdma_send_buffer[node_id][GET_THD_ID];
}

char* transport_t::get_buffer(uint32_t node_id, uint32_t qp_id) {
    return _rdma_send_buffer[qp_id][node_id];
    // return _rdma_send_buffer[node_id][qp_id];
}

char* transport_t::get_batch_buffer(){
    return _rdma_send_batch_buffer[GET_THD_ID][0];
    // return _rdma_send_batch_buffer[0][GET_THD_ID];
}

char* transport_t::get_batch_buffer(uint32_t node_id, uint32_t group_id) {
    return _rdma_send_batch_buffer[group_id][node_id];
}

char* transport_t::get_batch_buffer(uint32_t node_id) {
    return _rdma_send_batch_buffer[GET_THD_ID / batch_table_t::MAX_GROUP_SIZE][node_id];
    // return _rdma_send_batch_buffer[GET_THD_ID][node_id];
    // return _rdma_send_batch_buffer[node_id][GET_THD_ID];
}

char* transport_t::get_recv_buffer(uint32_t node_id) {
    return _rdma_recv_buffer[GET_THD_ID][node_id];
    // return _rdma_recv_buffer[node_id][GET_THD_ID];
}

char* transport_t::get_recv_buffer(uint32_t node_id, uint32_t qp_id) {
    return _rdma_recv_buffer[qp_id][node_id];
    // return _rdma_recv_buffer[node_id][qp_id];
}

char* transport_t::get_recv_batch_buffer(uint32_t node_id) {
    return _rdma_recv_batch_buffer[GET_THD_ID][node_id];
    // return _rdma_recv_batch_buffer[node_id][GET_THD_ID];
}

char* transport_t::get_recv_batch_buffer(uint32_t node_id, uint32_t qp_id) {
    return _rdma_recv_batch_buffer[qp_id][node_id];
    // return _rdma_recv_batch_buffer[node_id][qp_id];
}

uint64_t transport_t::serialize_node_info(uint32_t node_id, uint32_t qp_id) {
    return ((uint64_t)node_id << 32) | qp_id;
}

void transport_t::deserialize_node_info(uint64_t data, uint32_t& node_id, uint32_t& qp_id) {
    qp_id = data & 0xffffffff;
    node_id = (data >> 32);
}

bool transport_t::post_send(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, bool signaled) {
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    // wr.send_flags |= (size <= 512) ? IBV_SEND_INLINE : 0; // use inline if size is small
    
    if (ibv_post_send(qp, &wr, &wr_bad)) {
        int err = errno;
        debug::notify_error("Failed to ibv_post_send (RDMA SEND): %s (errno: %d)", strerror(err), err);
        // debug::notify_error("Failed to ibv_post_send (RDMA SEND)");
        
        assert(false);
        return false;
    }
    return true;
}

bool post_send_imm(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint32_t imm_data, bool signaled) {
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list    = &list;
    wr.num_sge    = 1;
    wr.imm_data   = imm_data;
    wr.opcode     = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.send_flags = (size <= 512) ? (wr.send_flags | IBV_SEND_INLINE) : wr.send_flags;
    
    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA SEND)");
        assert(false);
        return false;
    }
    return true;
}

bool transport_t::post_recv(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id) {
    struct ibv_sge list;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id = wr_id;
    wr.sg_list = &list;
    wr.num_sge = 1;

    if(ibv_post_recv(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_recv");
    	assert(false);
        return false;
    }
    return true;
}

bool transport_t::post_faa(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t add, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    assert(size <= 8);
    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey   = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = rkey;
    wr.wr.atomic.compare_add = add;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA FAA)");
    	assert(false);
        return false;
    }
    return true;
}

bool transport_t::post_faa_bound(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t add, uint64_t boundary, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
#if 0
    struct ibv_sge list;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr* wr_bad;

    assert(size <= 8);
    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = 1 << size;
    list.lkey   = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;

    wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_FETCH_AND_ADD;
    wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;
    if (signaled) wr.exp_send_flags |= IBV_SEND_SIGNALED;
    wr.ext_op.masked_atomics.remote_addr = dest;
    wr.ext_op.masked_atomics.rkey = rkey;
    wr.ext_op.masked_atomics.log_arg_sz = size;

    auto& op = wr.ext_op.masked_atomics.wr_data.inline_data.op.fetch_add;
    op.add_val = add;
    op.field_boundary = boundary;

    if(ibv_exp_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA FAA BOUNDARY)");
    	assert(false);
        return false;
    }
#endif
    return true;
}

bool transport_t::post_cas(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    assert(size <= 8);
    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey   = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = rkey;
    wr.wr.atomic.compare_add = expected;
    wr.wr.atomic.swap = desired;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA CAS)");
    	assert(false);
        return false;
    }
    return true;
}

bool transport_t::post_cas_mask(struct ibv_qp* qp, char* src, uint64_t dest, uint64_t expected, uint64_t desired, uint64_t mask, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
#if 0
    struct ibv_sge list;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr* wr_bad;

    assert(size <= 8);
    // memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = 1 << size;
    list.lkey   = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;

    wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
    wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;
    if (signaled) wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;

    wr.ext_op.masked_atomics.log_arg_sz = size;
    wr.ext_op.masked_atomics.remote_addr = dest;
    wr.ext_op.masked_atomics.rkey = rkey;
    
    auto& op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
    op.compare_val = expected;
    op.swap_val = desired;
    op.compare_mask = mask;
    op.swap_mask = mask;

    if(ibv_exp_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA CAS MASK)");
    	assert(false);
        return false;
    }
    return true;
#endif
    return false;
}

bool transport_t::post_read(struct ibv_qp* qp, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey   = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_READ;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = rkey;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA READ)");
    	assert(false);
        return false;
    }
    return true;
}

bool transport_t::post_write(struct ibv_qp* qp, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id      = 0;
    wr.sg_list    = &list;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = rkey;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA WRITE)");
    	assert(false);
        return false;
    }
    return true;
}

int transport_t::poll_cq(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc) {
    int total_wc = 0;
    while (total_wc < num_entries) {
        int ne = ibv_poll_cq(cq, num_entries - total_wc, wc + total_wc);
        if (ne < 0) {
            debug::notify_error("Failed to poll CQ");
            return -1;
        }
        total_wc += ne;
    }
    return total_wc;
}


int transport_t::poll_cq_once(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc) {
    int cnt = ibv_poll_cq(cq, num_entries, wc);
    if (cnt < 0) {
        debug::notify_error("Failed to ibv_poll_cq once ---- status %s (%d)", ibv_wc_status_str(wc->status), wc->status);
        assert(false);
    }
    return cnt;
}

void transport_t::rdma_faa(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t add, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    bool ret = post_faa(qp, src, dest, add, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
    }
}

void transport_t::rdma_faa_bound(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t add, uint64_t boundary, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    bool ret = post_faa_bound(qp, src, dest, add, boundary, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
    }
}

bool transport_t::rdma_cas(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t cmp, uint64_t swp, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    bool ret = post_cas(qp, src, dest, cmp, swp, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
        return cmp == *reinterpret_cast<uint64_t*>(src);
    }
    else
        return true;
}

bool transport_t::rdma_cas_mask(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint64_t cmp, uint64_t swp, uint64_t mask, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    bool ret = post_cas_mask(qp, src, dest, cmp, swp, mask, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);

        if (size <= 3) {
            return (cmp & mask) == (*reinterpret_cast<uint64_t*>(src) & mask);
        }
        else {
            uint64_t* eq = reinterpret_cast<uint64_t*>(cmp);
            uint64_t* old = reinterpret_cast<uint64_t*>(src);
            uint64_t* m = reinterpret_cast<uint64_t*>(mask);
            for (int i=0; i<(1 << (size - 3)); i++) {
                if ((eq[i] & m[i]) != (__bswap_64(old[i] & m[i]))) {
                    return false;
                }
            }
            return true;
        }
    }
    else
        return true;
}

void transport_t::rdma_read(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    post_read(qp, src, dest, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
    }
}

void transport_t::rdma_write(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint64_t dest, uint32_t size, uint32_t lkey, uint32_t rkey, bool signaled) {
    post_write(qp, src, dest, size, lkey, rkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
    }
}

void transport_t::rdma_send(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint32_t size, uint32_t lkey, bool signaled) {
    post_send(qp, src, size, lkey, signaled);
    if (signaled) {
        struct ibv_wc wc;
        poll_cq(cq, 1, &wc);
    }
}

// this is synchronous
void transport_t::rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id) {
    struct ibv_wc wc;
    post_recv(qp, src, size, lkey, wr_id);
    poll_cq(cq, 1, &wc);
    INC_INT_STATS(bytes_received, wc.byte_len);
    traffic_response += wc.byte_len;
}

void transport_t::rdma_recv_prepost(struct ibv_qp* qp, char* src, uint32_t size, uint32_t lkey, uint64_t wr_id) {
    post_recv(qp, src, size, lkey, wr_id);
}
