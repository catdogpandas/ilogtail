/*
 * Copyright 2024 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>

#include "BaseScheduler.h"
#include "common/http/HttpResponse.h"
#include "monitor/metric_models/MetricTypes.h"
#include "pipeline/queue/QueueKey.h"
#include "prometheus/PromSelfMonitor.h"
#include "prometheus/schedulers/ScrapeConfig.h"

#ifdef APSARA_UNIT_TEST_MAIN
#include "pipeline/queue/ProcessQueueItem.h"
#endif

namespace logtail {

struct PromTargetInfo {
    Labels mLabels;
    std::string mInstance;
    std::string mHash;
    std::string mHashForOperator;
    uint64_t mRebalanceMs = 0;
};

class ScrapeScheduler : public BaseScheduler {
public:
    ScrapeScheduler(std::shared_ptr<ScrapeConfig> scrapeConfigPtr,
                    std::string host,
                    int32_t port,
                    QueueKey queueKey,
                    size_t inputIndex,
                    const PromTargetInfo& targetInfo);
    ScrapeScheduler(const ScrapeScheduler&) = delete;
    ~ScrapeScheduler() override = default;

    void OnMetricResult(HttpResponse&, uint64_t timestampMilliSec);

    std::string GetId() const;
    std::string GetHashForOperator() const;

    void SetComponent(std::shared_ptr<Timer> timer, EventPool* eventPool);
    uint64_t GetLastScrapeSize() const { return mScrapeResponseSizeBytes; }

    uint64_t GetReBalanceMs() const { return mTargetInfo.mRebalanceMs; }
    void ScheduleNext() override;
    void ScrapeOnce(std::chrono::steady_clock::time_point execTime);
    void Cancel() override;
    void InitSelfMonitor(const MetricLabels&);

private:
    std::unique_ptr<TimerEvent> BuildScrapeTimerEvent(std::chrono::steady_clock::time_point execTime);

    std::shared_ptr<ScrapeConfig> mScrapeConfigPtr;
    std::string mHost;
    int32_t mPort;
    PromTargetInfo mTargetInfo;

    // pipeline
    QueueKey mQueueKey;
    size_t mInputIndex;

    // auto metrics
    std::atomic_uint64_t mScrapeResponseSizeBytes;

    // self monitor
    std::shared_ptr<PromSelfMonitorUnsafe> mSelfMonitor;
    MetricsRecordRef mMetricsRecordRef;
    CounterPtr mPromDelayTotal;
    CounterPtr mPluginTotalDelayMs;
#ifdef APSARA_UNIT_TEST_MAIN
    friend class ProcessorParsePrometheusMetricUnittest;
    friend class ScrapeSchedulerUnittest;
#endif
};

} // namespace logtail
