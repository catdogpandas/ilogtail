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
#include "plugin/processor/inner/ProcessorPromRelabelMetricNative.h"

#include <cstddef>
#include <json/json.h>

#include "common/Flags.h"
#include "common/StringTools.h"
#include "models/MetricEvent.h"
#include "models/PipelineEventGroup.h"
#include "models/PipelineEventPtr.h"
#include "models/SizedContainer.h"
#include "prometheus/Constants.h"

using namespace std;

DECLARE_FLAG_STRING(_pod_name_);

namespace logtail {

const string ProcessorPromRelabelMetricNative::sName = "processor_prom_relabel_metric_native";

// only for inner processor
bool ProcessorPromRelabelMetricNative::Init(const Json::Value& config) {
    std::string errorMsg;
    mScrapeConfigPtr = std::make_unique<ScrapeConfig>();
    if (!mScrapeConfigPtr->InitStaticConfig(config)) {
        return false;
    }

    mLoongCollectorScraper = STRING_FLAG(_pod_name_);

    return true;
}

void ProcessorPromRelabelMetricNative::Process(PipelineEventGroup& metricGroup) {
    // if mMetricRelabelConfigs is empty and honor_labels is true, skip it
    auto targetTags = metricGroup.GetTags();

    if (!mScrapeConfigPtr->mMetricRelabelConfigs.Empty() || !targetTags.empty()) {
        EventsContainer& events = metricGroup.MutableEvents();
        size_t wIdx = 0;
        for (size_t rIdx = 0; rIdx < events.size(); ++rIdx) {
            if (ProcessEvent(events[rIdx], targetTags)) {
                if (wIdx != rIdx) {
                    events[wIdx] = std::move(events[rIdx]);
                }
                ++wIdx;
            }
        }
        events.resize(wIdx);
    }

    if (metricGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_STREAM_TOTAL)) {
        auto autoMetric = prom::AutoMetric();
        UpdateAutoMetrics(metricGroup, autoMetric);
        AddAutoMetrics(metricGroup, autoMetric);
    }

    // delete all tags
    for (const auto& [k, v] : targetTags) {
        metricGroup.DelTag(k);
    }
}

bool ProcessorPromRelabelMetricNative::IsSupportedEvent(const PipelineEventPtr& e) const {
    return e.Is<MetricEvent>();
}

bool ProcessorPromRelabelMetricNative::ProcessEvent(PipelineEventPtr& e, const GroupTags& targetTags) {
    if (!IsSupportedEvent(e)) {
        return false;
    }
    auto& sourceEvent = e.Cast<MetricEvent>();

    auto& eventTags = sourceEvent.GetTags();

    for (const auto& [k, v] : targetTags) {
        auto it = eventTags.mInner.find(k);
        if (it != eventTags.mInner.end()) {
            if (!mScrapeConfigPtr->mHonorLabels) {
                // metric event labels is secondary
                // if confiliction, then rename it exported_<label_name>
                auto key = prometheus::EXPORTED_PREFIX + k.to_string();
                auto b = sourceEvent.GetSourceBuffer()->CopyString(key);
                eventTags.mInner[StringView(b.data, b.size)] = it->second;
                it->second = v;
                eventTags.mAllocatedSize += b.size + v.size();
            }
        } else {
            eventTags.mInner[k] = v;
            eventTags.mAllocatedSize += k.size() + v.size();
        }
    }

    if (!mScrapeConfigPtr->mMetricRelabelConfigs.Empty()
        && !mScrapeConfigPtr->mMetricRelabelConfigs.Process(sourceEvent)) {
        return false;
    }
    // set metricEvent name
    sourceEvent.SetNameNoCopy(sourceEvent.GetTag(prometheus::NAME));

    for (auto it = eventTags.mInner.begin(); it != eventTags.mInner.end();) {
        if (it->first.rfind("__", 0) == 0) {
            it = eventTags.mInner.erase(it);
            eventTags.mAllocatedSize -= it->first.size() + it->second.size();
        } else {
            ++it;
        }
    }

    for (const auto& [k, v] : mScrapeConfigPtr->mExternalLabels) {
        if (sourceEvent.HasTag(k)) {
            if (!mScrapeConfigPtr->mHonorLabels) {
                // metric event labels is secondary
                // if confiliction, then rename it exported_<label_name>
                auto key = prometheus::EXPORTED_PREFIX + k;
                auto b = sourceEvent.GetSourceBuffer()->CopyString(key);
                sourceEvent.SetTagNoCopy(StringView(b.data, b.size), sourceEvent.GetTag(k));
                sourceEvent.SetTagNoCopy(k, v);
            }
        } else {
            sourceEvent.SetTagNoCopy(k, v);
        }
    }

    // set metricEvent name
    sourceEvent.SetTagNoCopy(prometheus::NAME, sourceEvent.GetName());

    return true;
}

void ProcessorPromRelabelMetricNative::UpdateAutoMetrics(const PipelineEventGroup& eGroup,
                                                         prom::AutoMetric& autoMetric) const {
    if (eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_DURATION)) {
        autoMetric.mScrapeDurationSeconds
            = StringTo<double>(eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_DURATION).to_string());
    }
    if (eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_RESPONSE_SIZE)) {
        autoMetric.mScrapeResponseSizeBytes
            = StringTo<uint64_t>(eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_RESPONSE_SIZE).to_string());
    }
    autoMetric.mScrapeSamplesLimit = mScrapeConfigPtr->mSampleLimit;
    if (eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_SAMPLES_SCRAPED)) {
        autoMetric.mScrapeSamplesScraped
            = StringTo<uint64_t>(eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SAMPLES_SCRAPED).to_string());
    }
    autoMetric.mScrapeTimeoutSeconds = mScrapeConfigPtr->mScrapeTimeoutSeconds;

    if (eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_STATE)) {
        autoMetric.mScrapeState = eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_STATE).to_string();
    }

    if (eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_UP_STATE)) {
        autoMetric.mUp = StringTo<bool>(eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_UP_STATE).to_string());
    }
}

void ProcessorPromRelabelMetricNative::AddAutoMetrics(PipelineEventGroup& eGroup,
                                                      const prom::AutoMetric& autoMetric) const {
    auto targetTags = eGroup.GetTags();
    if (!eGroup.HasMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_TIMESTAMP_MILLISEC)) {
        LOG_ERROR(sLogger, ("scrape_timestamp_milliseconds is not set", ""));
        return;
    }

    StringView scrapeTimestampMilliSecStr = eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_TIMESTAMP_MILLISEC);
    auto timestampMilliSec = StringTo<uint64_t>(scrapeTimestampMilliSecStr.to_string());
    auto timestamp = timestampMilliSec / 1000;
    auto nanoSec = timestampMilliSec % 1000 * 1000000;


    AddMetric(
        eGroup, prometheus::SCRAPE_DURATION_SECONDS, autoMetric.mScrapeDurationSeconds, timestamp, nanoSec, targetTags);

    AddMetric(eGroup,
              prometheus::SCRAPE_RESPONSE_SIZE_BYTES,
              autoMetric.mScrapeResponseSizeBytes,
              timestamp,
              nanoSec,
              targetTags);

    if (autoMetric.mScrapeSamplesLimit > 0) {
        AddMetric(
            eGroup, prometheus::SCRAPE_SAMPLES_LIMIT, autoMetric.mScrapeSamplesLimit, timestamp, nanoSec, targetTags);
    }

    // AddMetric(eGroup,
    //           prometheus::SCRAPE_SAMPLES_POST_METRIC_RELABELING,
    //           autoMetric.mPostRelabel,
    //           timestamp,
    //           nanoSec,
    //           targetTags);

    AddMetric(
        eGroup, prometheus::SCRAPE_SAMPLES_SCRAPED, autoMetric.mScrapeSamplesScraped, timestamp, nanoSec, targetTags);

    AddMetric(
        eGroup, prometheus::SCRAPE_TIMEOUT_SECONDS, autoMetric.mScrapeTimeoutSeconds, timestamp, nanoSec, targetTags);

    AddMetric(eGroup, prometheus::SCRAPE_STATE, 1.0 * autoMetric.mUp, timestamp, nanoSec, targetTags);
    auto& last = eGroup.MutableEvents()[eGroup.GetEvents().size() - 1];
    auto scrapeState = eGroup.GetMetadata(EventGroupMetaKey::PROMETHEUS_SCRAPE_STATE);
    last.Cast<MetricEvent>().SetTag(METRIC_LABEL_KEY_STATUS, scrapeState);

    // up metric must be the last one
    AddMetric(eGroup, prometheus::UP, 1.0 * autoMetric.mUp, timestamp, nanoSec, targetTags);
}

void ProcessorPromRelabelMetricNative::AddMetric(PipelineEventGroup& metricGroup,
                                                 const string& name,
                                                 double value,
                                                 time_t timestamp,
                                                 uint32_t nanoSec,
                                                 const GroupTags& targetTags) const {
    auto* metricEvent = metricGroup.AddMetricEvent(true);
    metricEvent->SetName(name);
    metricEvent->SetValue<UntypedSingleValue>(value);
    metricEvent->SetTimestamp(timestamp, nanoSec);
    metricEvent->SetTag(prometheus::NAME, name);
    for (const auto& [k, v] : targetTags) {
        if (k.starts_with("__")) {
            continue;
        }
        metricEvent->SetTag(k, v);
    }
}

} // namespace logtail
