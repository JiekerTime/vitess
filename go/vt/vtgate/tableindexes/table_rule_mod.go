package tableindexes

var _ TableIndexRule = (*tableRuleMod)(nil)

type tableRuleMod struct {
}

func (t tableRuleMod) GetIndex(value string, tablesNum int) {
	panic("implement me")
}
