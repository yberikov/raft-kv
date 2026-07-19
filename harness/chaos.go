package harness

type ChaosConfig struct {
	DropP      float64
	DuplicateP float64
	MinDelay   int
	MaxDelay   int
}
