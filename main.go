package main

import (
	"context"
	"flag"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var (
	sourceUser   string
	sourcePwd    string
	destUser     string
	destPwd      string
	sourceIP     string
	sourcePort   int64
	destIP       string
	destPort     int64
	indexL       string
	keepTime     string
	numberShards float64
	dataNum      int64
	keepMapping  bool
	bulkNum      int64
	putNum       int64
	otype        string
	pipeID       string
	noindex      string
	parallel     int64
)

func main() {

	stringCH := make(chan string, 10)
	go indexList(stringCH)

	var exec sync.WaitGroup
	for i := 0; i < int(parallel); i++ {
		exec.Add(1)
		go func() {
			do(stringCH)
			exec.Done()
		}()
	}
	exec.Wait()

}

func do(ch chan string) {
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return
			}
			log.Println(v, ": begins")
			newDB().index(v).shards().mapping().channel().data()
			log.Println(v, ": end")
		}
	}
}

func newDB() *db {
	c := new(db)

	c.sourceCL, c.err = newCL(sourceIP, sourcePort, sourceUser, sourcePwd)
	if c.err != nil {
		c.err = errors.New(sourceIP + " : " + c.err.Error())
		return c
	}

	c.destCL, c.err = newCL(destIP, destPort, destUser, destPwd)
	if c.err != nil {
		c.err = errors.New(destIP + " : " + c.err.Error())
		return c
	}

	c.ctx = context.Background()

	var fwg sync.WaitGroup
	var twg sync.WaitGroup

	c.sourceWG = &fwg
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
	sourceCL  *elastic.Client
	destCL    *elastic.Client
	indexName string
	shard     int
	err       error
	ctx       context.Context
	result    chan []*elastic.SearchHit
	sourceWG  *sync.WaitGroup
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
	d.indexName = str
	return d
}

func (d *db) sliceQuery(num int) *elastic.ScrollService {
	return d.sourceCL.Scroll(d.indexName).KeepAlive(keepTime).
		Slice(elastic.NewSliceQuery().Id(num).Max(d.shard)).
		Size(int(bulkNum))
}

func (d *db) query() *elastic.ScrollService {
	return d.sourceCL.Scroll(d.indexName).
		KeepAlive(keepTime).Size(int(bulkNum))
}

func (d *db) sliceData(num int) {

	defer d.sourceWG.Done()
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
		_, err := elastic.NewClearScrollService(d.sourceCL).ScrollId(ids).Do(d.ctx)
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
			bulkRequest := d.destCL.Bulk()
			esRequest := make([]elastic.BulkableRequest, 0, putNum)
			num := int64(0)

			for i := range hitData {
				if num == putNum {
					_, err := bulkRequest.Add(esRequest...).Pipeline(pipeID).Refresh("false").Do(d.ctx)
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
				_, err := bulkRequest.Add(esRequest...).Pipeline(pipeID).Refresh("false").Do(d.ctx)
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
		d.sourceWG.Add(1)
		go d.sliceData(i)
	}

	d.sourceWG.Wait()
	close(d.result)
	d.toWG.Wait()

	if d.err != nil {
		log.Println(d.err)
	}
	return
}

func (d *db) createIndex() {

	node, err := elastic.NewNodesInfoService(d.destCL).Do(d.ctx)
	if err != nil {
		d.err = err
		return
	}

	body := make(map[string]interface{})
	body["index.number_of_shards"] = strconv.Itoa(int(math.Ceil(float64(len(node.Nodes)) * numberShards)))
	body["index.number_of_replicas"] = "0"

	tmp := make(map[string]interface{})
	tmp["settings"] = body

	_, err = d.destCL.CreateIndex(d.indexName).BodyJson(tmp).Do(d.ctx)

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

	mapping, err := d.sourceCL.GetMapping().Index(d.indexName).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	ok, err := d.destCL.IndexExists(d.indexName).Do(d.ctx)
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

	_, err = d.destCL.PutMapping().Index(d.indexName).
		BodyJson(mapping[d.indexName].(map[string]interface{})["mappings"].(map[string]interface{})).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	return d
}

func indexList(ch chan string) {

	defer close(ch)

	cl, err := newCL(sourceIP, sourcePort, sourceUser, sourcePwd)
	if err != nil {
		log.Println(sourceIP + " : " + err.Error())
		return
	}

	res, err := cl.CatIndices().Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}

	if len(res) == 0 {
		log.Println(errors.New("index len is 0"))
		return
	}

	must := strings.Split(indexL, ",")
	filter := strings.Split(noindex, ",")

	match := func(str string) bool {
		return smatch(must, str, true) && !smatch(filter, str, false)
	}

	for k := range res {
		if !strings.HasPrefix(res[k].Index, ".") && match(res[k].Index) {
			ch <- res[k].Index
		}
	}
}

func smatch(source []string, dest string, b bool) bool {
	if len(source) == 1 && source[0] == "" {
		return b
	}
	for i := range source {
		if strings.Index(source[i], "*") > 0 {
			return regexp.MustCompile(strings.ReplaceAll(source[i], "*", ".*")).MatchString(dest)
		}
		if source[i] == dest {
			return true
		}
	}
	return false
}

func (d *db) shards() *db {
	if d.err != nil {
		return d
	}
	slice, err := d.sourceCL.IndexGetSettings(d.indexName).Do(d.ctx)
	if err != nil {
		d.err = err
		return d
	}
	d.shard, err = strconv.Atoi(slice[d.indexName].Settings["index"].(map[string]interface{})["number_of_shards"].(string))
	if err != nil {
		d.err = err
	}
	return d
}

func init() {
	flag.Int64Var(&parallel, "p", 1, "Number of indexes executed in parallel")
	flag.StringVar(&noindex, "v", "", "Ignored index list a,b, use wildcards *")
	flag.StringVar(&pipeID, "pid", "", "Preprocessing pipeline ID of destination es")
	flag.Int64Var(&putNum, "pnum", 1000, "Number of single batch submissions(a single shard)")
	flag.Int64Var(&bulkNum, "bnum", 3000, "The amount of data read in batches(a single shard)")
	flag.Int64Var(&dataNum, "num", -1, "The amount of data to migrate an index (a single shard)."+
		"\n-1 all the data\n"+
		"The minimum amount of data is equal to -bnum\n"+
		"-99 No data is imported")
	flag.BoolVar(&keepMapping, "km", false, "Whether the original index map is not retained")
	flag.Float64Var(&numberShards, "mul", 1.0, "number shards multiple")
	flag.StringVar(&keepTime, "kt", "6s", "Read the data retention time of es ")
	flag.Int64Var(&sourcePort, "fp", 9200, "source es port")
	flag.StringVar(&sourceIP, "fi", "", "source es ip ")
	flag.Int64Var(&destPort, "tp", 9200, "dest es port")
	flag.StringVar(&destIP, "ti", "", "dest es ip")
	flag.StringVar(&sourceUser, "fu", "", "source user name")
	flag.StringVar(&sourcePwd, "fpwd", "", "source pwd")
	flag.StringVar(&destUser, "tu", "", "dest user name")
	flag.StringVar(&destPwd, "tpwd", "", "dest pwd")
	flag.StringVar(&indexL, "i", "", "index name list a,b . use wildcards *")
	flag.StringVar(&otype, "o", "index", "Resolution of _id conflict: index coverage; The create skip")
	flag.Parse()

	if sourceIP == "" || destIP == "" {
		log.Fatalln("please input sourceIP or destIP")
	}
}
