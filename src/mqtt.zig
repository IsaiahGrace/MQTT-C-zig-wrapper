const std = @import("std");
const atomic = std.atomic;
const os = std.os;
const mqtt = @cImport(@cInclude("mqtt.h"));

const AtomicBool = atomic.Atomic(bool);

pub const QoS = enum(c_int) {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExaclyOnce = 2,
};

pub const MqttClient = struct {
    const Self = @This();

    client: mqtt.mqtt_client,
    sendbuf: [1024]u8,
    recvbuf: [1024]u8,
    active: AtomicBool,
    connected: bool,
    ioThread: ?std.Thread,

    // Init must be provided with a callback function which handles incoming messages from subscribed topics.
    // A pointer to context will be passed as the first argument to the callback function.
    pub fn init() MqttClient {
        return .{
            .client = undefined,
            .sendbuf = undefined,
            .recvbuf = undefined,
            .active = AtomicBool.init(true),
            .connected = false,
            .ioThread = null,
        };
    }

    pub fn connect(self: *Self, ipAddr: []const u8, port: u16) !void {
        if (self.connected) {
            return error.AlreadyConnected;
        }

        const address = try std.net.Address.parseIp(ipAddr, port);

        self.client.socketfd = try os.socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
        errdefer os.closeSocket(self.client.socketfd);

        try os.connect(self.client.socketfd, &address.any, address.getOsSockLen());

        // Add the the socket non-blocking flag AFTER connecting!
        const flags = try os.fcntl(self.client.socketfd, os.system.F.GETFL, 0);
        _ = try os.fcntl(self.client.socketfd, os.system.F.SETFL, flags | os.SOCK.NONBLOCK);

        // Set the "state" pointer in the C struct to @This. That way the "static" callback above can access "member"
        // methods. This kinda feels like using a C library from C++! I've done similar things to get libcurl to play
        // nice with my C++ class. Does this mean that the struct is now uncopyable?! Should I use a zig "pinned"
        // keyword somewhere? https://github.com/ziglang/zig/issues/7769
        self.client.publish_response_callback_state = self;

        try mqttTry(mqtt.mqtt_init(&self.client, self.client.socketfd, &self.sendbuf, self.sendbuf.len, &self.recvbuf, self.recvbuf.len, pubRespCB));

        try mqttTry(mqtt.mqtt_connect(&self.client, "zig-mqtt", null, null, 0, null, null, mqtt.MQTT_CONNECT_CLEAN_SESSION, 30));

        self.connected = true;
    }

    pub fn disconnect(self: *Self) void {
        self.connected = false;
        self.active.store(false, std.builtin.AtomicOrder.Unordered);
        if (self.ioThread) |t| {
            t.join();
        }
        os.closeSocket(self.client.socketfd);
    }

    pub fn subscribe(self: *Self, topic: [:0]const u8, quality: QoS) !void {
        try self.assertConnected();
        try mqttTry(mqtt.mqtt_subscribe(&self.client, topic, @enumToInt(quality)));
    }

    pub fn handlePublishedMsg(self: *Self, topic: []const u8, msg: []const u8) void {
        // All the fancy C -> Zig pointer magic happens in the callback function, so now we have nice Zig slices to work with.
        _ = self;
        std.log.info("{s} = {s}", .{ topic, msg });
    }

    pub fn sync(self: *Self) !void {
        try self.assertConnected();
        try mqttTry(mqtt.mqtt_sync(&self.client));
    }

    // Starts a thread in the background to periodically call mqtt_sync
    pub fn startSyncThread(self: *Self) !void {
        try self.assertConnected();
        self.ioThread = try std.Thread.spawn(.{}, syncThread, .{self});
    }

    fn syncThread(self: *Self) void {
        std.log.debug("syncThread starting", .{});
        while (self.active.load(std.builtin.AtomicOrder.Unordered)) {
            std.time.sleep(1 * std.time.ns_per_s);
            mqttTry(mqtt.mqtt_sync(&self.client)) catch |e| {
                std.debug.panic("syncThread crash. mqttError returned by mqtt_sync = {}", .{e});
            };
        }
        std.log.debug("syncThread exiting", .{});
    }

    fn assertConnected(self: *Self) !void {
        if (!self.connected) {
            return error.NotConnected;
        }
    }

    fn mqttTry(mqttError: mqtt.MQTTErrors) !void {
        if (mqttError == mqtt.MQTT_OK) {
            return;
        }

        return switch (mqttError) {
            mqtt.MQTT_ERROR_UNKNOWN => error.MQTT_ERROR_UNKNOWN,
            mqtt.MQTT_ERROR_NULLPTR => error.MQTT_ERROR_NULLPTR,
            mqtt.MQTT_ERROR_CONTROL_FORBIDDEN_TYPE => error.MQTT_ERROR_CONTROL_FORBIDDEN_TYPE,
            mqtt.MQTT_ERROR_CONTROL_INVALID_FLAGS => error.MQTT_ERROR_CONTROL_INVALID_FLAGS,
            mqtt.MQTT_ERROR_CONTROL_WRONG_TYPE => error.MQTT_ERROR_CONTROL_WRONG_TYPE,
            mqtt.MQTT_ERROR_CONNECT_CLIENT_ID_REFUSED => error.MQTT_ERROR_CONNECT_CLIENT_ID_REFUSED,
            mqtt.MQTT_ERROR_CONNECT_NULL_WILL_MESSAGE => error.MQTT_ERROR_CONNECT_NULL_WILL_MESSAGE,
            mqtt.MQTT_ERROR_CONNECT_FORBIDDEN_WILL_QOS => error.MQTT_ERROR_CONNECT_FORBIDDEN_WILL_QOS,
            mqtt.MQTT_ERROR_CONNACK_FORBIDDEN_FLAGS => error.MQTT_ERROR_CONNACK_FORBIDDEN_FLAGS,
            mqtt.MQTT_ERROR_CONNACK_FORBIDDEN_CODE => error.MQTT_ERROR_CONNACK_FORBIDDEN_CODE,
            mqtt.MQTT_ERROR_PUBLISH_FORBIDDEN_QOS => error.MQTT_ERROR_PUBLISH_FORBIDDEN_QOS,
            mqtt.MQTT_ERROR_SUBSCRIBE_TOO_MANY_TOPICS => error.MQTT_ERROR_SUBSCRIBE_TOO_MANY_TOPICS,
            mqtt.MQTT_ERROR_MALFORMED_RESPONSE => error.MQTT_ERROR_MALFORMED_RESPONSE,
            mqtt.MQTT_ERROR_UNSUBSCRIBE_TOO_MANY_TOPICS => error.MQTT_ERROR_UNSUBSCRIBE_TOO_MANY_TOPICS,
            mqtt.MQTT_ERROR_RESPONSE_INVALID_CONTROL_TYPE => error.MQTT_ERROR_RESPONSE_INVALID_CONTROL_TYPE,
            mqtt.MQTT_ERROR_CONNECT_NOT_CALLED => error.MQTT_ERROR_CONNECT_NOT_CALLED,
            mqtt.MQTT_ERROR_SEND_BUFFER_IS_FULL => error.MQTT_ERROR_SEND_BUFFER_IS_FULL,
            mqtt.MQTT_ERROR_SOCKET_ERROR => error.MQTT_ERROR_SOCKET_ERROR,
            mqtt.MQTT_ERROR_MALFORMED_REQUEST => error.MQTT_ERROR_MALFORMED_REQUEST,
            mqtt.MQTT_ERROR_RECV_BUFFER_TOO_SMALL => error.MQTT_ERROR_RECV_BUFFER_TOO_SMALL,
            mqtt.MQTT_ERROR_ACK_OF_UNKNOWN => error.MQTT_ERROR_ACK_OF_UNKNOWN,
            mqtt.MQTT_ERROR_NOT_IMPLEMENTED => error.MQTT_ERROR_NOT_IMPLEMENTED,
            mqtt.MQTT_ERROR_CONNECTION_REFUSED => error.MQTT_ERROR_CONNECTION_REFUSED,
            mqtt.MQTT_ERROR_SUBSCRIBE_FAILED => error.MQTT_ERROR_SUBSCRIBE_FAILED,
            mqtt.MQTT_ERROR_CONNECTION_CLOSED => error.MQTT_ERROR_CONNECTION_CLOSED,
            mqtt.MQTT_ERROR_INITIAL_RECONNECT => error.MQTT_ERROR_INITIAL_RECONNECT,
            mqtt.MQTT_ERROR_INVALID_REMAINING_LENGTH => error.MQTT_ERROR_INVALID_REMAINING_LENGTH,
            mqtt.MQTT_ERROR_CLEAN_SESSION_IS_REQUIRED => error.MQTT_ERROR_CLEAN_SESSION_IS_REQUIRED,
            mqtt.MQTT_ERROR_RECONNECT_FAILED => error.MQTT_ERROR_RECONNECT_FAILED,
            mqtt.MQTT_ERROR_RECONNECTING => error.MQTT_ERROR_RECONNECTING,
            else => {
                std.log.err("MQTTError is not in the enum! {d}", .{mqttError});
                return error.UnknownMQTTErrorValue;
            },
        };
    }
};

// The callback function for handing incoming messages cannot be a bound function, because we need to pass a pointer of
// it to the mqtt library code. But helpfully, the library gives us the "state" pointer which we can do anything with.
// We set it to @This in connect(), so now we have a pointer back to the original Zig struct!
fn pubRespCB(state: [*c]?*anyopaque, publish: [*c]mqtt.mqtt_response_publish) callconv(.C) void {
    // Pointer manipulation at it's finest!
    var self: *MqttClient = @ptrCast(*MqttClient, state);
    var packet: *mqtt.mqtt_response_publish = @ptrCast(*mqtt.mqtt_response_publish, publish);

    // Extract the void* C pointers and cast them as many-item pointers
    const topicPtr = @ptrCast([*]const u8, packet.topic_name);
    const msgPtr = @ptrCast([*]const u8, packet.application_message);

    // Create slices from the many-item pointers to pass to handlePublishedMsg()
    const topic = topicPtr[0..packet.topic_name_size];
    const msg = msgPtr[0..packet.application_message_size];

    self.handlePublishedMsg(topic, msg);
}

// The two functions below implement the IO requirements of the mqtt-c library used. They are linked with the C code in
// mqtt.c, and together allow us to forgo linking against libc.

export fn mqtt_pal_sendall(fd: mqtt.mqtt_pal_socket_handle, rawBuf: ?*anyopaque, len: usize, flags: c_int) callconv(.C) isize {
    const buf = @ptrCast([*]const u8, rawBuf);
    const slice = buf[0..len];
    var sent: usize = 0;

    while (sent < len) {
        sent += os.send(fd, slice[sent..len], @intCast(u32, flags)) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return mqtt.MQTT_ERROR_SOCKET_ERROR,
        };
    }
    return @intCast(isize, sent);
}

export fn mqtt_pal_recvall(fd: mqtt.mqtt_pal_socket_handle, rawBuf: ?*anyopaque, bufsz: usize, flags: c_int) callconv(.C) isize {
    const buf = @ptrCast([*]u8, rawBuf);
    const slice = buf[0..bufsz];

    const recvd = os.recv(fd, slice, @intCast(u32, flags)) catch |err| switch (err) {
        error.WouldBlock => return 0,
        else => return mqtt.MQTT_ERROR_SOCKET_ERROR,
    };

    if (recvd == 0) {
        return mqtt.MQTT_ERROR_SOCKET_ERROR;
    }

    return @intCast(isize, recvd);
}
