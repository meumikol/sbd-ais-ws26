package main

import "fmt"

// Greeting is a struct holding the data we want to print.
type Greeting struct {
	Message string
}

// Print is a method with a value receiver of type Greeting.
func (g Greeting) Print() {
	fmt.Println(g.Message)
}

func main() {
	g := Greeting{Message: "Hello, World!"}
	g.Print()
}
