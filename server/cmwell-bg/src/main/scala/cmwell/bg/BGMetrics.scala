/**
  * © 2019 Refinitiv. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package cmwell.bg

import nl.grons.metrics4.scala._
import com.codahale.metrics.{
  Counter => DropwizardCounter,
  Histogram => DropwizardHistogram,
  Meter => DropwizardMeter,
  Timer => DropwizardTimer
}
import scala.collection.JavaConverters._

class BGMetrics extends DefaultInstrumented {

  /** *** Metrics *****/
  val existingMetrics = metricRegistry.getMetrics.asScala
  val writeCommandsCounter: Counter = existingMetrics
    .get("WriteCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("WriteCommand Counter"))
  val updatePathCommandsCounter: Counter = existingMetrics
    .get("UpdatePathCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("UpdatePathCommand Counter"))
  val deletePathCommandsCounter: Counter = existingMetrics
    .get("DeletePathCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("DeletePathCommand Counter"))
  val deleteAttributesCommandsCounter: Counter = existingMetrics
    .get("DeleteAttributesCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("DeleteAttributesCommand Counter"))
  val overrideCommandCounter: Counter = existingMetrics
    .get("OverrideCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("OverrideCommand Counter"))
  val indexNewInfotonCommandCounter: Counter = existingMetrics
    .get("IndexNewCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("IndexNewCommand Counter"))
  val indexExistingCommandCounter: Counter = existingMetrics
    .get("IndexExistingCommand Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("IndexExistingCommand Counter"))
  val mergeTimer: Timer = existingMetrics
    .get("Merge Timer")
    .map { m =>
      new Timer(m.asInstanceOf[DropwizardTimer])
    }
    .getOrElse(metrics.timer("Merge Timer"))
  val commandMeter: Meter = existingMetrics
    .get("Commands Meter")
    .map { m =>
      new Meter(m.asInstanceOf[DropwizardMeter])
    }
    .getOrElse(metrics.meter("Commands Meter"))
  val infotonCommandWeightHist: Histogram = existingMetrics
    .get("WriteCommand OverrideCommand Infoton Weight Histogram")
    .map { m =>
      new Histogram(m.asInstanceOf[DropwizardHistogram])
    }
    .getOrElse(metrics.histogram("WriteCommand OverrideCommand Infoton Weight Histogram"))
  val indexingTimer: Timer = existingMetrics
    .get("Indexing Timer")
    .map { m =>
      new Timer(m.asInstanceOf[DropwizardTimer])
    }
    .getOrElse(metrics.timer("Indexing Timer"))
  val casFullReadTimer: Timer = existingMetrics
    .get("CAS Full Read Timer")
    .map { m =>
      new Timer(m.asInstanceOf[DropwizardTimer])
    }
    .getOrElse(metrics.timer("CAS Full Read Timer"))
  val casEmptyReadTimer: Timer = existingMetrics
    .get("CAS Empty Read Timer")
    .map { m =>
      new Timer(m.asInstanceOf[DropwizardTimer])
    }
    .getOrElse(metrics.timer("CAS Empty Read Timer"))
  val nullUpdateCounter: Counter = existingMetrics
    .get("NullUpdate Counter")
    .map { m =>
      new Counter(m.asInstanceOf[DropwizardCounter])
    }
    .getOrElse(metrics.counter("NullUpdate Counter"))
  val indexBulkSizeHist: Histogram = existingMetrics
    .get("Index Bulk Size Histogram")
    .map { m =>
      new Histogram(m.asInstanceOf[DropwizardHistogram])
    }
    .getOrElse(metrics.histogram("Index Bulk Size Histogram"))
}
