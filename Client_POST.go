package main

import (
    "encoding/json"
    "fmt"
    "net/http"
	"bufio"
	"bytes"
	// "io"
	"os"
	// "ioutil"
	"io/ioutil"
	"strconv"
	"strings"
)

func main() {

	for {
		fmt.Println("Enter Choice : \n 1. Add Restaurant\n 2. Search Restaurant\n 3. Remove Restaurant")
		clientReader := bufio.NewReader(os.Stdin)
		clientRequest, err := clientReader.ReadString('\n')
		if err != nil {
			fmt.Print(err)
		}
		input_val, err := strconv.Atoi(strings.TrimRight(clientRequest, "\r\n"))
		if err != nil {
			fmt.Print(err)
		}

		if input_val == 1 {
			fmt.Println("Enter name : ")
			rest_name, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}

			fmt.Println("Online Delivery(Yes or No) : ")
			rest_online_delivery, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Enter URL : ")
			rest_url, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Enter Price range : ")
			rest_price_range, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Enter Cuisines : ")
			rest_cuisines, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Delivering Now(Yes or No) : ")
			rest_delivery_now, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Restaurant Rating ")
			rest_user_rating, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			
	
			restaurant := make(map[string]interface{})
			restaurant["name"] = rest_name
			restaurant["online_delivery"] = rest_online_delivery
			restaurant["url"] = rest_url
			restaurant["price_range"] = rest_price_range
			restaurant["cuisines"] = rest_cuisines
			restaurant["delivery_now"] = rest_delivery_now
			restaurant["user_rating"] = rest_user_rating
			
			
			fmt.Println("Address : ")
			rest_address, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("City (Hyderabad or Bangalore) : ")
			rest_city, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Country Id : ")
			rest_country, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Locality : ")
			rest_locality, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			
	
			restaurant_Add := make(map[string]interface{})
			restaurant_Add["Address"] = rest_address
			restaurant_Add["City"] = rest_city
			restaurant_Add["country"] = rest_country
			restaurant_Add["locality"] = rest_locality
			
			restaurant["address"] = restaurant_Add
	
			postBody, _ := json.Marshal(restaurant)
			responseBody := bytes.NewBuffer(postBody)

			
			fmt.Println("Enter port : ")
			port, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			
	
			resp, err := http.Post("http://127.0.0.1:" + port + "/key", "application/json", responseBody)
			// fmt.Printf("http://127.0.0.1:"+string(p)+"/key")
			if err != nil {
				fmt.Print(err)
			}
			fmt.Print("================aux=====================\n")
			fmt.Print(resp)
		} else if input_val == 2 {
			fmt.Println("Enter Restaurant Name : ")
			search_keyword, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}

			search_keyword = strings.TrimRight(search_keyword, "\r\n")
			resp, err := http.Get("http://127.0.0.1:11000/key/" + search_keyword)
	
			if err != nil {
				panic(err)
			}
	
			defer resp.Body.Close()
	
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			type Restaurant_ struct {
				Id int64 `json:"id"`
				name string `json:"name"`
				online_delivery string `json:"online_delivery"`
				url string `json:"url"`
				price_range string `json:"price_range"`
				cuisines string `json:"cuisines"`
				delivery_now string `json:"delivery_now"`
				user_rating int64 `json:"user_rating"`
			}

			var rstrnt = new(Restaurant_)
			err1 := json.Unmarshal(body, &rstrnt)
			if(err1 != nil){
				fmt.Println("whoops:", err1)
			}
			fmt.Println(rstrnt.delivery_now)
	
			fmt.Println(string(body))
		} else if input_val == 3 {
			fmt.Println("Enter Restaurant Name : ")
			restaurant_name_delete, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println("Enter Restaurant City(Hyderabad or Bangalore) : ")
			restaurant_loc_delete, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			restaurant_name_delete = strings.TrimRight(restaurant_name_delete, "\r\n")
			restaurant_loc_delete = strings.TrimRight(restaurant_loc_delete, "\r\n")
			client := &http.Client{}
			req, err := http.NewRequest("DELETE", "http://127.0.0.1:11000/key/" + restaurant_name_delete + "/" + restaurant_loc_delete, nil)
			 resp, err := client.Do(req)
			    if err != nil {
			        fmt.Println(err)
			        return
			    }
			    defer resp.Body.Close()
		} else {
			fmt.Println("Incorrect Input!")
			hold_screen, err := clientReader.ReadString('\n')
			if err != nil {
				fmt.Print(err)
			}
			fmt.Println(hold_screen)
		}
	}
}