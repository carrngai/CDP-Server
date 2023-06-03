package cdp

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

type DataAccessLayer struct {
	db *sql.DB
}

func NewDataAccessLayer(db *sql.DB) *DataAccessLayer {
	return &DataAccessLayer{db: db}
}

func (dal *DataAccessLayer) CreateData(tableName string, values ...interface{}) error {
	query := buildInsertQuery(tableName, values)

	stmt, err := dal.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(values...)
	if err != nil {
		return err
	}

	return nil
}

func (dal *DataAccessLayer) ReadData(tableName string, id int, dest ...interface{}) error {
	query := buildSelectQuery(tableName)

	stmt, err := dal.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	return stmt.QueryRow(id).Scan(dest...)
}

func (dal *DataAccessLayer) UpdateData(tableName string, values ...interface{}) error {
	query := buildUpdateQuery(tableName, values)

	stmt, err := dal.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(values...)
	if err != nil {
		return err
	}

	return nil
}

func (dal *DataAccessLayer) DeleteData(tableName string, id int) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", tableName)

	stmt, err := dal.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}

	return nil
}

func buildInsertQuery(tableName string, values []interface{}) string {
	numValues := len(values)
	valuePlaceholders := make([]string, numValues)
	for i := 0; i < numValues; i++ {
		valuePlaceholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, joinString(valuePlaceholders, ", "))
	return query
}

func buildSelectQuery(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE id = $1", tableName)
}

func buildUpdateQuery(tableName string, values []interface{}) string {
	numValues := len(values)
	setClauses := make([]string, numValues)
	for i := 0; i < numValues; i++ {
		setClauses[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", tableName, joinString(setClauses, ", "), numValues)
	return query
}

func joinString(strs []string, separator string) string {
	return "'" + fmt.Sprintf(strings.Join(strs, separator)) + "'"
}

func test() {
	// Establish a connection to the PostgreSQL database
	db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/mydatabase?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping the database to ensure the connection is successful
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	dal := NewDataAccessLayer(db)

	// Example usage with a specific data structure
	type User struct {
		ID   int
		Name string
	}

	user := User{
		Name: "John Doe",
	}

	tableName := "users"

	// Create a new user
	err = dal.CreateData(tableName, user.ID, user.Name)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("User created successfully")

	// Read user data
	readUser := User{}
	err = dal.ReadData(tableName, user.ID, &readUser.ID, &readUser.Name)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Read user: ID: %d, Name: %s\n", readUser.ID, readUser.Name)

	// Update user data
	readUser.Name = "Jane Doe"
	err = dal.UpdateData(tableName, readUser.ID, readUser.Name)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("User updated successfully")

	// Delete user data
	err = dal.DeleteData(tableName, readUser.ID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("User deleted successfully")
}
