package smda

// func (db *Database) setupRoutes() {
// func (db *Database) RunWebserver(port int) {

// func TestPortEnsuring(t *testing.T) {
// 	db, err := NewDatabaseTemp()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer func() {
// 	if err := db.Drop(); err != nil {
// 		panic(err)
// 	}
// }()

// 	port := 1234
// 	go func() {
// 		db.RunWebserver(port, true)
// 	}()
// defer func() {db.server.Shutdown(context.TODO())}

// db2, err := NewDatabaseTemp()
// ... (err handling, cleanup etc.)
// 	// this should fail, but it's hard to test atm, because we log.Fatal there instead of an error
// 	// we don't return errors from this function, because it blocks (ListenAndServe) - a new architecture
// 	// is needed for a proper test suite
// 	db2.RunWebserver(port, true)
// }
