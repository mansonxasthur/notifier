package domain

// Scenario is how the simulated provider answers one send.
type Scenario string

const (
	ScenarioSucceed       Scenario = "succeed"
	ScenarioFailPermanent Scenario = "fail_permanent"
	ScenarioFailTransient Scenario = "fail_transient"
	ScenarioSlow          Scenario = "slow"
	ScenarioTimeout       Scenario = "timeout"
	ScenarioLateFailure   Scenario = "late_failure"
)

// Scenarios lists every scenario.
func Scenarios() []Scenario {
	return []Scenario{
		ScenarioSucceed, ScenarioFailPermanent, ScenarioFailTransient,
		ScenarioSlow, ScenarioTimeout, ScenarioLateFailure,
	}
}

// Valid reports whether s is a known scenario.
func (s Scenario) Valid() bool {
	switch s {
	case ScenarioSucceed, ScenarioFailPermanent, ScenarioFailTransient,
		ScenarioSlow, ScenarioTimeout, ScenarioLateFailure:
		return true
	}
	return false
}
