/*
 * Stream Connect Direct Kafka — Inbound Script Consumer
 * Deployed at: sys_kafka_script_consumer "CMDB CI Inbound", field: event_consumer
 * Topic: cmdb.ci.inbound  ->  cmdb_ci  (idempotent upsert by correlation_id)
 *
 * Runtime contract: receives `messages` = [ { key, message, headers } ].
 * serialization_format = text, so message is the raw string -> JSON.parse here.
 */
(function process(messages) {
    for (var i = 0; i < messages.length; i++) {
        try {
            var m = JSON.parse(messages[i].message);
            var cid = m.correlation_id || m.sys_id;

            var gr = new GlideRecord('cmdb_ci');
            gr.addQuery('correlation_id', cid);
            gr.query();
            var found = gr.next();
            if (!found) {
                gr.initialize();
                gr.setValue('correlation_id', cid);
            }

            if (m.name) gr.setValue('name', m.name);
            if (m.operational_status) gr.setValue('operational_status', m.operational_status);
            if (m.short_description) gr.setValue('short_description', m.short_description);
            gr.setValue('sys_class_name', m.sys_class_name || 'cmdb_ci');

            var id = found ? (gr.update(), gr.getUniqueValue()) : gr.insert();
            gs.info('[CMDB_CI_INBOUND] ' + (found ? 'updated' : 'inserted') + ' CI ' + id + ' correlation_id=' + cid);
        } catch (e) {
            gs.error('[CMDB_CI_INBOUND] error: ' + e + ' raw=' + messages[i].message);
        }
    }
})(messages);
