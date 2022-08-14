package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"syscall"
	"time"

	"github.com/antchfx/htmlquery"
	"github.com/google/uuid"

	"github.com/xuri/excelize/v2"
)

type boardItem struct {
	Text        string `json:"text"`
	Tel         string `json:"tel"`
	Name        string `json:"name"`
	City        string `json:"city"`
	Category    string `json:"category"`
	SubCategory string `json:"subcategory"`
	Cost        string `json:"cost"`
	Title       string `json:"title"`
	Photos      string `json:"photos"`
}

var pages chan int
var queue chan boardItem
var sitelink string
var filter string
var processPhotos bool
var pagesCount int
var rate int

func init() {

}

func main() {

	flag.StringVar(&sitelink, "link", "https://berkat.ru/board?page=", "link to site or category")
	flag.StringVar(&filter, "filter", "", "filter for categories")
	flag.BoolVar(&processPhotos, "photo", false, "process photos")
	flag.IntVar(&pagesCount, "pages", 5, "number of pages")
	flag.IntVar(&rate, "rate", 200, "rate limiter milliseconds")
	flag.Parse()

	fmt.Println(`Usage: -photo  if you need photos ,
		link=https://berkat.ru/board?page= ,
		filter=some filter ,
		pages=5`)

	flag.PrintDefaults()
	if processPhotos {
		err := os.Mkdir("photos", 0755)
		if err != nil {
			log.Println(err)
		}
	}

	workersNum := 20
	start := time.Now()

	var wg sync.WaitGroup
	sigs := make(chan os.Signal)
	ctx, cancleFunc := context.WithCancel(context.Background())
	defer cancleFunc()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		cancleFunc()
		fmt.Println(sig)
	}()

	limiter := time.Tick(time.Millisecond * time.Duration(rate))
	go func(ctx context.Context) {
		for i := 0; i < pagesCount; i++ {
			select {

			case <-ctx.Done():
				close(pages)
				fmt.Println(ctx.Err())
				return
			default:
				<-limiter
				pages <- i
			}
		}
		close(pages)
	}(ctx)

	pages = make(chan int, 20)
	queue = make(chan boardItem, 20)

	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for page := range pages {
				pageTask(page)
			}
		}()
	}

	go func() {
		wg.Wait()
		for range pages {
		}
		close(queue)

	}()

	f, err := os.Create("b.json")
	if err != nil {
		log.Fatalln("error open file")
	}
	fcsv, err := os.Create("b.csv")
	if err != nil {
		log.Fatalln("error open file")
	}
	defer f.Close()
	defer fcsv.Close()
	w := csv.NewWriter(fcsv)

	writer := bufio.NewWriter(f)
	writer.WriteString("[\n")
	cell := 1

	fxls := excelize.NewFile()
	defer func() {
		// Close the spreadsheet.
		if err := fxls.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	for v := range queue {
		jboard, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			log.Println("error convert")
			continue
		}
		writer.Write(jboard)
		w.Write([]string{v.Tel, v.Text, v.Name, v.Cost, v.City, v.Category, v.SubCategory})
		writer.WriteString(",\n")
		cellStr := strconv.Itoa(cell)
		fxls.SetCellValue("Sheet1", "A"+cellStr, v.Tel)
		fxls.SetCellValue("Sheet1", "B"+cellStr, v.Text)
		fxls.SetCellValue("Sheet1", "C"+cellStr, v.Name)
		fxls.SetCellValue("Sheet1", "D"+cellStr, v.Cost)
		fxls.SetCellValue("Sheet1", "E"+cellStr, v.City)
		fxls.SetCellValue("Sheet1", "F"+cellStr, v.Category)
		fxls.SetCellValue("Sheet1", "G"+cellStr, v.SubCategory)
		fxls.SetCellValue("Sheet1", "H"+cellStr, v.Title)
		fxls.SetCellValue("Sheet1", "I"+cellStr, v.Photos)
		cell++
	}
	writer.Flush()
	f.Seek(int64(-len(",\n")), io.SeekEnd)
	fi, _ := f.Stat()
	f.Truncate(fi.Size() - int64(len(",\n")))
	writer.WriteString("\n]")
	writer.Flush()
	w.Flush()
	if err := fxls.SaveAs("Book1.xlsx"); err != nil {
		fmt.Println(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("==========> %s\n", elapsed)
}

func pageTask(page int) {
	var ph string
	//doc, err := htmlquery.LoadURL(fmt.Sprintf("%s%d", "https://berkat.ru/board?page=", page+1))
	// doc, err := htmlquery.LoadURL("https://berkat.ru/board?page=" + strconv.Itoa(page+1))
	// "https://berkat.ru/avto/legkovye-avtomobili?page="
	// "&title=&city=&p1=88&p2=1591&p5%5Bfrom%5D=&p5%5Bto%5D=&p10%5Bfrom%5D=&p10%5Bto%5D=&p21=&p16%5Bfrom%5D=&p16%5Bto%5D=&submit=Фильтровать"
	doc, err := htmlquery.LoadURL(sitelink + strconv.Itoa(page+1) + filter)
	if err != nil {
		log.Println(`Cannot load URL`)
		return
	}

	nodes, err := htmlquery.QueryAll(doc, `//div[contains(@id,"board_list_item")]`)

	if err != nil {
		log.Println(`not a valid XPath expression.`)
		return
	}
	for _, v := range nodes {
		var name string
		var city string
		var cost string
		var title string

		replacer := strings.NewReplacer(`"`, " ", `'`, " ")
		tel := htmlquery.InnerText(htmlquery.FindOne(v, `//a[@class="get_phone_style"]`))
		tel = strings.ReplaceAll(tel, "-", "")
		if n, err := htmlquery.Query(v, `//div[@class="board_list_footer_left"]/span[6]`); err == nil && n != nil {
			name = replacer.Replace(strings.TrimSpace(htmlquery.InnerText(n)))
		} else {
			name = ""
		}
		text := htmlquery.InnerText(htmlquery.FindOne(v, `//p[@class="board_list_item_text"]`))
		text = replacer.Replace(text)
		if c, err := htmlquery.Query(v, `//div[@class="board_list_footer_left"]/span[7]`); err == nil && c != nil {
			city = replacer.Replace(strings.TrimSpace(htmlquery.InnerText(c)))
		} else {
			city = ""
		}

		if t, err := htmlquery.Query(v, `//*[@class="board_list_item_title"]/a`); err == nil && t != nil {
			title = replacer.Replace(strings.TrimSpace(htmlquery.InnerText(t)))
		} else {
			title = ""
		}

		category := htmlquery.InnerText(htmlquery.FindOne(v, `//div[@class="board_list_footer_left"]/span[1]`))
		category = replacer.Replace(category)
		subcategory := htmlquery.InnerText(htmlquery.FindOne(v, `//div[@class="board_list_footer_left"]/span[2]`))
		subcategory = replacer.Replace(subcategory)
		if s, err := htmlquery.Query(v, `//div[@class="board_list_footer_left"]/span[contains(text(),"Цена")]`); err == nil && s != nil {
			cost = replacer.Replace(htmlquery.InnerText(s))
		} else {
			cost = ""
		}

		if processPhotos {
			var photoArr []string
			photos, err := htmlquery.QueryAll(v, `//*[contains(@id,"photos")]/a`)
			if err != nil {
				log.Println(err)
			}

			for _, n := range photos {
				a := n
				if a != nil {
					href := htmlquery.SelectAttr(a, "href")
					href = "https://berkat.ru" + href

					sarr := strings.Split(href, "/")
					id := uuid.New().String()
					filename := id + sarr[len(sarr)-1]
					photoArr = append(photoArr, filename)
					ph = strings.Join(photoArr, ",")
					downloadFile(href, "./photos/"+filename)
				}
			}
		}
		queue <- boardItem{Text: text, Tel: tel, Name: name, City: city, Category: category, SubCategory: subcategory, Cost: cost, Title: title, Photos: ph}
	}
	log.Println("Page ", page+1)
}

func downloadFile(URL, fileName string) error {
	//Get the response bytes from the url
	response, err := http.Get(URL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.New("received non 200 response code")
	}
	//Create a empty file
	file, err := os.Create(fileName)
	if err != nil {
		log.Println(err)
		return err
	}
	defer file.Close()

	//Write the bytes to the fiel
	_, err = io.Copy(file, response.Body)
	if err != nil {
		return err
	}

	return nil
}
