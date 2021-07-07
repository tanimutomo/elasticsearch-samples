package dao

import (
	"context"
	"fmt"
	"reflect"

	"github.com/k0kubun/pp"
	elastic "github.com/olivere/elastic/v7"
	"github.com/tanimutomo/elasticsearch-samples/structured/pkg/entity"
)

var (
	setting = `
{
	"settings":{
		"number_of_shards": 2,
		"number_of_replicas": 0
	}
}`
	mapping = `
{
	"properties":{
		"user":{
			"type":"keyword"
		},
		"message":{
			"type":"text",
			"store": true,
			"fielddata": true
		},
		"image":{
			"type":"keyword"
		},
		"created":{
			"type":"date"
		},
		"tags":{
			"type":"keyword"
		},
		"location":{
			"type":"geo_point"
		},
		"suggest_field":{
			"type":"completion"
		}
	}
}`
)

type Twitter struct {
	es *elastic.Client
}

func NewTwitter(es *elastic.Client) Twitter {
	return Twitter{
		es: es,
	}
}

func (t Twitter) CreateIndex(ctx context.Context) error {
	exists, err := t.es.IndexExists("twitter").Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		_, err := t.es.DeleteIndex("twitter").Do(ctx)
		if err != nil {
			return err
		}
	}
	_, err = t.es.CreateIndex("twitter").BodyString(setting).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (t Twitter) PutMapping(ctx context.Context) error {
	_, err := t.es.PutMapping().Index("twitter").BodyString(mapping).Do(ctx)
	if err != nil {
		return err
	}

	twitterMap, err := t.es.GetMapping().Index("twitter").Do(ctx)
	if err != nil {
		return err
	}
	pp.Println("twitterMap:", twitterMap)

	return nil
}

func (t Twitter) CreateDocument(ctx context.Context) error {
	// Index a tweet (using JSON serialization)
	tweet1 := entity.Tweet{User: "olivere", Message: "Take Five", Retweets: 0}
	put1, err := t.es.Index().
		Index("twitter").
		// Type("tweet").
		Id("1").
		BodyJson(tweet1).
		Do(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	// Get tweet with specified ID
	get1, err := t.es.Get().
		Index("twitter").
		// Type("tweet").
		Id("1").
		Do(ctx)
	if err != nil {
		return err
	}
	if get1.Found {
		fmt.Printf("Got document %s in version %d from index %s, type %s\n", get1.Id, get1.Version, get1.Index, get1.Type)
	}

	return nil
}

func (t Twitter) SearchDocument(ctx context.Context) error {
	searchResult, err := t.es.Search().
		Index("twitter"). // search in index "twitter"
		Query(elastic.NewMatchAllQuery()).
		// Sort("user", true). // sort by "user" field, ascending
		// From(0).Size(10).   // take documents 0-9
		Pretty(true). // pretty print request and response JSON
		Do(ctx)       // execute
	if err != nil {
		return err
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ttyp entity.Tweet
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		if t, ok := item.(entity.Tweet); ok {
			fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
		}
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())

	return nil
}
