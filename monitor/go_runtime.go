package monitor

type goRuntime struct{}

func (g *goRuntime) Statistics() (map[string]interface{}, error) {
	return nil, nil
}

func (g *goRuntime) Diagnostics() (map[string]interface{}, error) {
	return nil, nil
}
