package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

type Cliente struct {
	Nrocliente int
	Nombre     string
	Apellido   string
	Domicilio  string
	Telefono   string
}

type Tarjeta struct {
	Nrotarjeta   string
	Nrocliente   int
	Validadesde  string
	Validahasta  string
	Codseguridad string
	Limitecompra float64
	Estado       string
}

type Comercio struct {
	Nrocomercio  int
	Nombre       string
	Domicilio    string
	Codigopostal string
	Telefono     string
}

type Compra struct {
	Nrooperacion int
	Nrotarjeta   string
	Nrocomercio  int
	Fecha        string
	Monto        float64
	Pagado       bool
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	// abre transacción de escritura
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}

	// cierra transacción
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})

	return buf, err
}

func guardarClientes() {
	db, err := bolt.Open("manejo-de-tarjetas.db", 0600, nil) //Open creates and opens a database at the given path. If the file does not exist then it will be created automatically. Passing in nil options will cause Bolt to open the database with the default options.
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	println("Datos de cliente:")

	// Marshaleo de cliente

	cliente := Cliente{1, "Jose", "San Martin", "Pujol 1960", "11-3958-0889"}
	data, err := json.Marshal(cliente)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)), data)

	resultado, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)))

	fmt.Printf("%s\n\n", resultado)

	cliente = Cliente{2, "Lionel", "Messi", "Eiffel 1987", "11-4714-4876"}
	data, err = json.Marshal(cliente)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)), data)

	resultado, err = ReadUnique(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)))

	fmt.Printf("%s\n\n", resultado)

	cliente = Cliente{1, "Pedro", "Perez", "Marquez 6875", "11-5873-5867"}
	data3, err := json.Marshal(cliente)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)), data3)

	resultado, err = ReadUnique(db, "cliente", []byte(strconv.Itoa(cliente.Nrocliente)))

	fmt.Printf("%s\n\n", resultado)

}

func guardarTarjetas() {
	db, err := bolt.Open("manejo-de-tarjetas.db", 0600, nil) //Open creates and opens a database at the given path. If the file does not exist then it will be created automatically. Passing in nil options will cause Bolt to open the database with the default options.
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	println("Datos de tarjeta:")

	// Marshaleo de tarjeta

	tarjeta := Tarjeta{"4424586923689485", 1, "202108", "202308", "9867", 40000.0, "vigente"}
	data, err := json.Marshal(tarjeta)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "tarjeta", []byte(tarjeta.Nrotarjeta), data)

	resultado, err := ReadUnique(db, "tarjeta", []byte(tarjeta.Nrotarjeta))

	fmt.Printf("%s\n\n", resultado)

	tarjeta = Tarjeta{"4268576829345681", 1, "201903", "202201", "0978", 30000.05, "suspendida"}
	data, err = json.Marshal(tarjeta)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "tarjeta", []byte(tarjeta.Nrotarjeta), data)

	resultado, err = ReadUnique(db, "tarjeta", []byte(tarjeta.Nrotarjeta))

	fmt.Printf("%s\n\n", resultado)

	tarjeta = Tarjeta{"4244985547281001", 2, "202201", "202401", "1238", 100000.99, "anulada"}
	data, err = json.Marshal(tarjeta)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "tarjeta", []byte(tarjeta.Nrotarjeta), data)

	resultado, err = ReadUnique(db, "tarjeta", []byte(tarjeta.Nrotarjeta))

	fmt.Printf("%s\n\n", resultado)
}

func guardarComercios() {
	db, err := bolt.Open("manejo-de-tarjetas.db", 0600, nil) //Open creates and opens a database at the given path. If the file does not exist then it will be created automatically. Passing in nil options will cause Bolt to open the database with the default options.
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	println("Datos de comercio:")

	// Marshaleo de comercio

	comercio := Comercio{1, "Aluar", "Pasteur 4600", "B1644AMV", "47258000"}
	data, err := json.Marshal(comercio)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)), data)

	resultado, err := ReadUnique(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)))

	fmt.Printf("%s\n\n", resultado)

	comercio = Comercio{2, "Central Puerto", "Av. Tomas Alva Edison", "B1644AMV", "43175000"}
	data, err = json.Marshal(comercio)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)), data)

	resultado, err = ReadUnique(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)))

	fmt.Printf("%s\n\n", resultado)

	comercio = Comercio{3, "Sociedad Comercial del Plata", "Colectora Panamericana 1804", "B1607EEV", "1121526000"}
	data, err = json.Marshal(comercio)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)), data)

	resultado, err = ReadUnique(db, "comercio", []byte(strconv.Itoa(comercio.Nrocomercio)))

	fmt.Printf("%s\n\n", resultado)
}

func guardarCompras() {
	db, err := bolt.Open("manejo-de-tarjetas.db", 0600, nil) //Open creates and opens a database at the given path. If the file does not exist then it will be created automatically. Passing in nil options will cause Bolt to open the database with the default options.
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	println("Datos de compra:")

	// Marshaleo de compra

	compra := Compra{1, "4424586923689485", 1, "2022-02-25", 1790.5, true}
	data, err := json.Marshal(compra)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)), data)

	resultado, err := ReadUnique(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)))

	fmt.Printf("%s\n\n", resultado)

	compra = Compra{2, "4268576829345681", 2, "2022-03-05", 45000.6, false}
	data, err = json.Marshal(compra)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)), data)

	resultado, err = ReadUnique(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)))

	fmt.Printf("%s\n\n", resultado)

	compra = Compra{3, "4244985547281001", 3, "2022-05-17", 5000.33, false}
	data, err = json.Marshal(compra)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)), data)

	resultado, err = ReadUnique(db, "compra", []byte(strconv.Itoa(compra.Nrooperacion)))

	fmt.Printf("%s\n\n", resultado)
}

func menu() {

	var opciones = `*****************************************
|                Ingrese                |
|-1: salir                              |
| 0: guardar datos de clientes          |
| 1: guardar datos de tarjetas          |
| 2: guardar datos de comercios         |
| 3: guardar datos de compras           |
*****************************************`

	var opcion int
	salir := false

	fmt.Println(opciones)

	for !salir {

		fmt.Scanf("%d", &opcion)

		switch opcion {
		case -1:
			salir = true
		case 0:
			guardarClientes()
		case 1:
			guardarTarjetas()
		case 2:
			guardarComercios()
		case 3:
			guardarCompras()
		default:
			fmt.Printf("La opción %v es INVÁLIDA.\n\n", opcion)
		}
	}

}

func main() {
	menu()
}
