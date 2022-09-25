package wal

type Log struct {
	opts      *Option
	writer    IWal
	fistIndex uint64
	lastIndex uint64
}

func Open(path string, opts *Option) (*Log, error) {
	var err error

	l := &Log{}
	l.writer, err = OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	err = l.writer.Check()
	if err != nil {
		return nil, err
	}
	first, err := l.writer.First()
	if err != nil {
		return nil, err
	}
	l.fistIndex = first.index

	last, err := l.writer.Last()
	if err != nil {
		return nil, err
	}
	l.lastIndex = last.index

	return nil, nil
}
