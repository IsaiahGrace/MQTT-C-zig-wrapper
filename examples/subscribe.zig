const std = @import("std");
const mqttClient = @import("mqtt");

pub fn main() anyerror!void {
    var client = mqttClient.MqttClient.init();

    try client.connect("127.0.0.1", 1883);
    defer client.disconnect();

    try client.subscribe("$SYS/#", .ExaclyOnce);

    try client.startSyncThread();

    std.log.info("going to sleep", .{});
    std.time.sleep(60 * std.time.ns_per_s);

    std.log.info("All done!", .{});
}
