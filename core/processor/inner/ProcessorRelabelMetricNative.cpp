/*
 * Copyright 2024 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "processor/inner/ProcessorRelabelMetricNative.h"

#include <json/json.h>

#include <cstddef>

#include "common/StringTools.h"
#include "models/MetricEvent.h"
#include "models/PipelineEventGroup.h"
#include "models/PipelineEventPtr.h"
#include "prometheus/Constants.h"

using namespace std;
namespace logtail {

const string ProcessorRelabelMetricNative::sName = "processor_relabel_metric_native";
const static StringView sINSTANCE = StringView("instance");

// only for inner processor
bool ProcessorRelabelMetricNative::Init(const Json::Value& config) {
    LOG_INFO(sLogger, ("processor init", config.toStyledString()));
    std::string errorMsg;
    if (config.isMember(prometheus::METRIC_RELABEL_CONFIGS) && config[prometheus::METRIC_RELABEL_CONFIGS].isArray()
        && config[prometheus::METRIC_RELABEL_CONFIGS].size() > 0) {
        for (const auto& item : config[prometheus::METRIC_RELABEL_CONFIGS]) {
            mRelabelConfigs.emplace_back(item);
            if (!mRelabelConfigs.back().Validate()) {
                errorMsg = "metric_relabel_configs is invalid";
                LOG_ERROR(sLogger, ("init prometheus processor failed", errorMsg));
                return false;
            }
        }
    }

    if (config.isMember(prometheus::JOB_NAME) && config[prometheus::JOB_NAME].isString()) {
        mJobName = config[prometheus::JOB_NAME].asString();
    } else {
        return false;
    }
    if (config.isMember(prometheus::SCRAPE_TIMEOUT) && config[prometheus::SCRAPE_TIMEOUT].isString()) {
        string tmpScrapeTimeoutString = config[prometheus::SCRAPE_TIMEOUT].asString();
        if (EndWith(tmpScrapeTimeoutString, "s")) {
            mScrapeTimeoutSeconds = stoll(tmpScrapeTimeoutString.substr(0, tmpScrapeTimeoutString.find('s')));
        } else if (EndWith(tmpScrapeTimeoutString, "m")) {
            mScrapeTimeoutSeconds = stoll(tmpScrapeTimeoutString.substr(0, tmpScrapeTimeoutString.find('m'))) * 60;
        }
    }
    if (config.isMember(prometheus::SAMPLE_LIMIT) && config[prometheus::SAMPLE_LIMIT].isInt64()) {
        mSampleLimit = config[prometheus::SAMPLE_LIMIT].asInt64();
    } else {
        mSampleLimit = -1;
    }
    if (config.isMember(prometheus::SERIES_LIMIT) && config[prometheus::SERIES_LIMIT].isInt64()) {
        mSeriesLimit = config[prometheus::SERIES_LIMIT].asInt64();
    } else {
        mSeriesLimit = -1;
    }

    return true;
}

void ProcessorRelabelMetricNative::Process(PipelineEventGroup& metricGroup) {
    if (metricGroup.GetEvents().empty()) {
        return;
    }

    StringView currentInstance = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_INSTANCE);
    EventsContainer& events = metricGroup.MutableEvents();

    size_t wIdx = 0;
    for (size_t rIdx = 0; rIdx < events.size(); ++rIdx) {
        if (ProcessEvent(events[rIdx], currentInstance)) {
            if (wIdx != rIdx) {
                events[wIdx] = std::move(events[rIdx]);
            }
            ++wIdx;
        }
    }
    events.resize(wIdx);

    // self monitor
    AddSelfMonitorMetrics(metricGroup);
}

bool ProcessorRelabelMetricNative::IsSupportedEvent(const PipelineEventPtr& e) const {
    return e.Is<MetricEvent>();
}

bool ProcessorRelabelMetricNative::ProcessEvent(PipelineEventPtr& e, StringView currentInstance) {
    if (!IsSupportedEvent(e)) {
        return false;
    }
    auto& sourceEvent = e.Cast<MetricEvent>();

    Labels labels;

    labels.Reset(&sourceEvent);
    Labels result;

    // if keep this sourceEvent
    if (prometheus::Process(labels, mRelabelConfigs, result)) {
        // if k/v in labels by not result, then delete it
        labels.Range([&result, &sourceEvent](const Label& label) {
            if (result.Get(label.name).empty()) {
                sourceEvent.DelTag(StringView(label.name));
            }
        });

        // for each k/v in result, set it to sourceEvent
        result.Range([&sourceEvent](const Label& label) { sourceEvent.SetTag(label.name, label.value); });

        // set metricEvent name
        if (!result.Get("__name__").empty()) {
            sourceEvent.SetName(result.Get("__name__"));
        }
        SetJobAndInstanceTag(sourceEvent, currentInstance);

        return true;
    }
    return false;
}

void ProcessorRelabelMetricNative::AddSelfMonitorMetrics(PipelineEventGroup& metricGroup) {
    // if up is set, then add self monitor metrics
    if (metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_UP_STATE).empty()) {
        return;
    }

    StringView scrapeTimestampStr = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_TIMESTAMP);
    auto scrapeTimestamp = StringTo<time_t>(scrapeTimestampStr.to_string());

    StringView scrapeDurationNanoSecondsStr = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_DURATION);
    double scrapeDurationSeconds = StringTo<double>(scrapeDurationNanoSecondsStr.to_string()) / 1000 / 1000 / 1000;

    StringView scrapeResponseSizeStr = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_RESPONSE_SIZE);
    auto scrapeResponseSize = StringTo<uint64_t>(scrapeResponseSizeStr.to_string());

    uint64_t samplesPostMetricRelabel = metricGroup.GetEvents().size();

    auto samplesScraped
        = StringTo<uint64_t>(metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SAMPLES_SCRAPED).to_string());

    uint64_t upState = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_UP_STATE).to_string() == "1" ? 1 : 0;

    MetricEvent* e = nullptr;
    StringView instance = metricGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_INSTANCE);

    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::SCRAPE_DURATION_SECONDS);
    e->SetValue<UntypedSingleValue>(scrapeDurationSeconds);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::SCRAPE_RESPONSE_SIZE_BYTES);
    e->SetValue<UntypedSingleValue>(scrapeResponseSize * 1.0);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    if (mSampleLimit > 0) {
        e = metricGroup.AddMetricEvent();
        e->SetName(prometheus::SCRAPE_SAMPLES_LIMIT);
        e->SetValue<UntypedSingleValue>(mSampleLimit * 1.0);
        e->SetTimestamp(scrapeTimestamp);
        SetJobAndInstanceTag(*e, instance);
    }

    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::SCRAPE_SAMPLES_POST_METRIC_RELABELING);
    e->SetValue<UntypedSingleValue>(samplesPostMetricRelabel * 1.0);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::SCRAPE_SAMPLES_SCRAPED);
    e->SetValue<UntypedSingleValue>(samplesScraped * 1.0);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::SCRAPE_TIMEOUT_SECONDS);
    e->SetValue<UntypedSingleValue>(mScrapeTimeoutSeconds * 1.0);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    // up metric must be the last one
    e = metricGroup.AddMetricEvent();
    e->SetName(prometheus::UP);
    e->SetValue<UntypedSingleValue>(upState * 1.0);
    e->SetTimestamp(scrapeTimestamp);
    SetJobAndInstanceTag(*e, instance);

    e = nullptr;
}

void ProcessorRelabelMetricNative::SetJobAndInstanceTag(MetricEvent& event, StringView instance) {
    event.SetTag(prometheus::JOB, mJobName);
    event.SetTag(sINSTANCE, instance);
}

} // namespace logtail
