package main

import (
	"context"
	"flag"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
)

var (
	fromUser     string
	fromPwd      string
	toUser       string
	toPwd        string
	fromIP       string
	fromPort     int64
	toIP         string
	toPort       int64
	indexL       string
	keepTime     string
	numberShards float64
	dataNum      int64
	keepMapping  bool
	bulkNum      int64
	putNum       int64
	otype        string
)

func main() {
	NewDB().Do()
}

func NewDB() *db {
	c := new(db)

	c.fromCL, c.err = newCL(fromIP, fromPort, fromUser, fromPwd)
	if c.err != nil {
		c.err = errors.New(fromIP + " : " + c.err.Error())
		return c
	}

	c.toCL, c.err = newCL(toIP, toPort, toUser, toPwd)
	if c.err != nil {
		c.err = errors.New(fromIP + " : " + c.err.Error())
		return c
	}

	c.ctx = context.Background()

	var fwg sync.WaitGroup
	var twg sync.WaitGroup

	c.fromWG = &fwg
	c.toWG = &twg
	return c

}

func newCL(ip string, port int64, user, pwd string) (*elastic.Client, error) {
	host := []string{"http://" + ip + ":" + strconv.Itoa(int(port))}
	client, err := elastic.NewClient(elastic.SetURL(host...),
		elastic.SetBasicAuth(user, pwd),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(true),
		elastic.SetMaxRetries(5))
	return client, err
}

type db struct {
	fromCL    *elastic.Client
	toCL      *elastic.Client
	indexNmae string
	shard     int
	err       error
	ctx       context.Context
	result    chan []*elastic.SearchHit
	fromWG    *sync.WaitGroup
	toWG      *sync.WaitGroup
}

func (d *db) channel() *db {
	if d.err != nil {
		return d
	}
	d.result = make(chan []*elastic.SearchHit, 10)

	return d
}

func (d *db) index(str string) *db {
	if d.err != nil {
		return d
	}
	d.indexNmae = str
	log.Println(d.indexNmae)
	return d
}

func (d *db) Do() {
	for _, v := range d.indexList() {
		d.index(v).shards().mapping().channel().data()
		d.err = nil
	}
}

func (d *db) sliceQuery(num int) *elastic.ScrollService {
	return d.fromCL.Scroll(d.indexNmae).KeepAlive(keepTime).
		Slice(elastic.NewSliceQuery().Id(num).Max(d.shard)).
		Size(int(bulkNum))
}

func (d *db) query() *elastic.ScrollService {
	return d.fromCL.Scroll(d.indexNmae).
		KeepAlive(keepTime).Size(int(bulkNum))
}

func (d *db) sliceData(num int) {

	defer d.fromWG.Done()
	var ids string
	var svc *elastic.ScrollService

	if d.shard == 1 {
		svc = d.query()
	} else {
		svc = d.sliceQuery(num)
	}

	vnum := int64(0)

	for {

		vnum++

		if dataNum >= 0 && vnum*bulkNum > dataNum {
			break
		}

		res, err := svc.Do(d.ctx)

		if err == io.EOF {
			break
		}
		if err != nil {
			d.err = err
			break
		}
		if res == nil {
			break
		}
		if res.Hits == nil {
			break
		}
		if res.Hits.TotalHits.Value == 0 {
			break
		}
		d.result <- res.Hits.Hits
		ids = res.ScrollId

	}

	if ids != "" {
		_, err := elastic.NewClearScrollService(d.fromCL).ScrollId(ids).Do(d.ctx)
		if err != nil {
			log.Println(err)
		}
	}

}

func (d *db) toBson(hits *elastic.SearchHit) elastic.BulkableRequest {

	return elastic.NewBulkIndexRequest().
		UseEasyJSON(true).Index(hits.Index).
		Routing(hits.Routing).Id(hits.Id).
		Doc(hits.Source).OpType(otype)
}

func (d *db) slicePostData() {
	defer d.toWG.Done()

	for {
		select {
		case hitData, ok := <-d.result:
			if !ok {
				return
			}
			bulkRequest := d.toCL.Bulk()
			esRequest := make([]elastic.BulkableRequest, 0, putNum)
			num := int64(0)

			for i := range hitData {
				if num == putNum {
					_, err := bulkRequest.Add(esRequest...).Refresh("false").Do(d.ctx)
					if err != nil {
						log.Println(err)
					}
					esRequest = esRequest[:0]
					num = 0
				}
				esRequest = append(esRequest, d.toBson(hitData[i]))
				num++
			}

			if len(esRequest) > 0 {
				_, err := bulkRequest.Add(esRequest...).Refresh("false").Do(d.ctx)
				if err != nil {
					log.Println(err)
				}
			}
		}
	}

}

func (d *db) data() {
	if d.err != nil {
		log.Println(d.err)
		return
	}

	if dataNum == -99 {
		return
	}

	for i := 0; i < d.shard*2; i++ {
		d.toWG.Add(1)
		go d.slicePostData()
	}

	for i := 0; i < d.shard; i++ {
		d.fromWG.Add(1)
		go d.sliceData(i)
	}

	d.fromWG.Wait()
	close(d.result)
	d.toWG.Wait()

	if d.err != nil {
		log.Println(d.err)
	}
	return
}

func (d *db) createIndex() {

	node, err := elastic.NewNodesInfoService(d.toCL).Do(d.ctx)
	if err != nil {
		d.err = err
		return
	}

	body := make(map[string]interface{})
	body["index.number_of_shards"] = strconv.Itoa(int(math.Ceil(float64(len(node.Nodes)) * numberShards)))
	body["index.number_of_replicas"] = "0"

	tmp := make(map[string]interface{})
	tmp["settings"] = body

	_, err = d.toCL.CreateIndex(d.indexNmae).BodyJson(tmp).Do(d.ctx)

	if err != nil {
		d.err = err
	}
}

func (d *db) mapping() *db {
	if d.err != nil {
		return d
	}

	if keepMapping {
		return d
	}

	mapping, err := d.fromCL.GetMapping().Index(d.indexNmae).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	ok, err := d.toCL.IndexExists(d.indexNmae).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}

	if !ok {
		d.createIndex()
	}

	if err != nil {
		return d
	}

	_, err = d.toCL.PutMapping().Index(d.indexNmae).
		BodyJson(mapping[d.indexNmae].(map[string]interface{})["mappings"].(map[string]interface{})).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	return d
}

func (d *db) indexList() []string {

	if d.err != nil {
		log.Println(d.err)
		return nil
	}

	if indexL != "" {
		return strings.Split(indexL, ",")
	}

	res, err := d.fromCL.CatIndices().Do(d.ctx)
	if err != nil {
		d.err = err
		return nil
	}
	dd := make([]string, 0, len(res))
	for k := range res {
		if !strings.HasPrefix(res[k].Index, ".") {
			dd = append(dd, res[k].Index)
		}
	}
	return dd
}

func (d *db) shards() *db {
	if d.err != nil {
		return d
	}
	slice, err := d.fromCL.IndexGetSettings(d.indexNmae).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	d.shard, err = strconv.Atoi(slice[d.indexNmae].Settings["index"].(map[string]interface{})["number_of_shards"].(string))
	if err != nil {
		d.err = err
	}
	return d
}

func init() {
	flag.Int64Var(&putNum, "pnum", 1000, "Number of single batch submissions(a single shard)")
	flag.Int64Var(&bulkNum, "bnum", 3000, "The amount of data read in batches(a single shard)")
	flag.Int64Var(&dataNum, "num", -1, "The amount of data to migrate an index (a single shard)."+
		"\n-1 all the data\n"+
		"The minimum amount of data is equal to -bnum\n"+
		"-99 No data is imported")
	flag.BoolVar(&keepMapping, "km", false, "Whether the original index map is not retained")
	flag.Float64Var(&numberShards, "mul", 1.0, "number shards multiple")
	flag.StringVar(&keepTime, "kt", "6s", "Read the data retention time of es ")
	flag.Int64Var(&fromPort, "fp", 9200, "es port")
	flag.StringVar(&fromIP, "fi", "", "es ip ")
	flag.Int64Var(&toPort, "tp", 9200, "es port")
	flag.StringVar(&toIP, "ti", "", " es ip")
	flag.StringVar(&fromUser, "fu", "", " user name")
	flag.StringVar(&fromPwd, "fpwd", "", " pwd")
	flag.StringVar(&toUser, "tu", "", " user name")
	flag.StringVar(&toPwd, "tpwd", "", " pwd")
	flag.StringVar(&indexL, "i", "", "index name list a,b")
	flag.StringVar(&otype, "o", "index", "Resolution of _id conflict: index coverage; The create skip")
	flag.Parse()
}
