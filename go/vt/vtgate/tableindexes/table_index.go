package tableindexes

type TableIndexRule interface {
	GetIndex(value string, tablesNum int)
}
