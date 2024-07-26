#include "RemoteWriteSerializer.h"

#include "prometheus/prompb/remote.pb.h"
#include "prometheus/prompb/types.pb.h"

using namespace std;

namespace logtail {

bool RemoteWriteEventGroupSerializer::Serialize(BatchedEvents&& p, std::string& res, std::string& errorMsg) {
    prometheus::WriteRequest req;

    for (auto& event : p.mEvents) {
        MetricEvent* metricEvent = event.Get<MetricEvent>();
        if (metricEvent == nullptr) {
            errorMsg = "event is not a metric event";
            LOG_ERROR(sLogger, ("event is not a metric event", errorMsg));
            continue;
        }
        auto* ts = req.add_timeseries();

        for (auto it = metricEvent->TagsBegin(); it != metricEvent->TagsEnd(); ++it) {
            auto* l = ts->add_labels();
            l->set_name(it->first.to_string());
            l->set_value(it->second.to_string());
        }
    
        // check `__name__` label
        if (metricEvent->GetTag("__name__").empty()) {
            auto* l = ts->add_labels();
            l->set_name("__name__");
            l->set_value(metricEvent->GetName().to_string());
        }

        auto* s = ts->add_samples();
        // 好像每个event只有一个sample
        s->set_value(metricEvent->GetValue<UntypedSingleValue>()->mValue);

        s->set_timestamp(metricEvent->GetTimestamp() * 1000);
    }
    req.SerializeToString(&res);
    return true;
}

} // namespace logtail
