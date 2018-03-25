package cmwell.bg.test

import org.scalatest.{DoNotDiscover, SequentialNestedSuiteExecution, Suites}

class BGSpecs extends Suites (
  new BGMergerSpec,
  new BGSeqSpecs
)

@DoNotDiscover
class BGSeqSpecs extends Suites(
  new CmwellBGSpec,
  new BGResilienceSpec,
  new BGSequentialSpec
) with SequentialNestedSuiteExecution
