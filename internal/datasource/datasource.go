package datasource

import "github.com/tddhit/hunter/types"

type DataSource interface {
	ReadChan() chan *types.Document
}
