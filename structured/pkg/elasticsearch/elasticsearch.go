package elasticsearch

import (
	"context"
	"fmt"

	elastic "github.com/olivere/elastic/v7"
)

var hosts = []string{
	"http://localhost:9200",
	"http://localhost:9201",
}

var es *elastic.Client

func Init() error {
	var err error
	es, err = elastic.NewClient(
		elastic.SetURL(hosts...),
		elastic.SetSniff(false),
	)
	info, code, err := es.Ping(hosts[0]).Do(context.TODO())
	if err != nil {
		return err
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	return nil
}

func Get() *elastic.Client {
	return es
}
