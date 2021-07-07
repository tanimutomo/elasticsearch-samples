package main

import (
	"context"
	"fmt"

	"github.com/tanimutomo/elasticsearch-samples/structured/pkg/dao"
	"github.com/tanimutomo/elasticsearch-samples/structured/pkg/elasticsearch"
)

func main() {
	if err := elasticsearch.Init(); err != nil {
		panic(err)
	}
	twt := dao.NewTwitter(elasticsearch.Get())
	ctx := context.Background()
	fmt.Println("CreateIndex: Error:", twt.CreateIndex(ctx))
	fmt.Println("PutMapping: Error:", twt.PutMapping(ctx))
	fmt.Println("CreateDocument: Error:", twt.CreateDocument(ctx))
	fmt.Println("SearchDocument: Error:", twt.SearchDocument(ctx))
}
