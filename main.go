package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run your_script.go sourceDSN destDSN")
		os.Exit(1)
	}

	sourceDSN := os.Args[1]
	destDSN := os.Args[2]

	// Open connections to source and destination databases
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatalf("Error connecting to source database: %v", err)
	}
	defer sourceDB.Close()

	destDB, err := sql.Open("mysql", destDSN)
	if err != nil {
		log.Fatalf("Error connecting to destination database: %v", err)
	}
	defer destDB.Close()

	// Get a list of tables in the source database
	rows, err := sourceDB.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("Error fetching tables from source database: %v", err)
	}
	defer rows.Close()

	// Loop through each table and copy data from source to destination
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatalf("Error scanning table name: %v", err)
		}

		// Check if the table exists in the destination database
		tableExists := tableExists(destDB, tableName)

		if tableExists {
			fmt.Printf("Table '%s' already exists in the destination database. Do you want to overwrite its data? (yes/no): ", tableName)
			var overwrite string
			_, err = fmt.Scan(&overwrite)
			if err != nil {
				log.Fatalf("Error reading user input: %v", err)
			}

			if strings.ToLower(overwrite) != "yes" {
				fmt.Printf("Skipping table %s.\n", tableName)
				continue
			}
		}

		// Copy table data from source to destination
		tx, err := destDB.Begin()
		if err != nil {
			log.Fatalf("Error starting transaction for table %s: %v", tableName, err)
		}

		_, err = tx.Exec(fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", tableName, tableName))
		println(fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", tableName, tableName))
		if err != nil {
			tx.Rollback()
			log.Fatalf("Error copying data for table %s: %v", tableName, err)
		}

		if err := tx.Commit(); err != nil {
			log.Fatalf("Error committing transaction for table %s: %v", tableName, err)
		}

		fmt.Printf("Data for table %s copied successfully.\n", tableName)
	}

	fmt.Println("All data copied successfully.")
}

// Check if a table exists in the database
func tableExists(db *sql.DB, tableName string) bool {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tableName).Scan(&count)
	if err != nil {
		log.Fatalf("Error checking if table %s exists: %v", tableName, err)
	}
	return count > 0
}
