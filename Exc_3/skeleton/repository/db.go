package repository

import (
	"ordersystem/model"
	"sync"
	"time"
)

type DatabaseHandler struct {
	mu sync.RWMutex
	// drinks represent all available drinks
	drinks []model.Drink
	// orders serves as order history
	orders []model.Order
}

func NewDatabaseHandler() *DatabaseHandler {
	// Init the drinks slice with some test data
	drinks := []model.Drink{
		{ID: 1, Name: "Beer", Price: 3.50, Description: "Cold lager, 0.5l"},
		{ID: 2, Name: "Wine", Price: 4.80, Description: "Glass of red wine"},
		{ID: 3, Name: "Mojito", Price: 7.20, Description: "Rum, mint, lime and soda"},
		{ID: 4, Name: "Water", Price: 1.50, Description: "Still mineral water"},
	}

	// Init orders slice with some test data
	now := time.Now()
	orders := []model.Order{
		{DrinkID: 1, Amount: 2, CreatedAt: now.Add(-3 * time.Hour)},
		{DrinkID: 2, Amount: 1, CreatedAt: now.Add(-2 * time.Hour)},
		{DrinkID: 1, Amount: 3, CreatedAt: now.Add(-90 * time.Minute)},
		{DrinkID: 3, Amount: 2, CreatedAt: now.Add(-1 * time.Hour)},
		{DrinkID: 4, Amount: 1, CreatedAt: now.Add(-30 * time.Minute)},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// return a copy so callers can't race with AddOrder
	out := make([]model.Order, len(db.orders))
	copy(out, db.orders)
	return out
}

// GetTotalledOrders calculates total orders
// key = DrinkID, value = Amount of orders
func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	totalledOrders := make(map[uint64]uint64)
	for _, o := range db.orders {
		totalledOrders[o.DrinkID] += o.Amount
	}
	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.orders = append(db.orders, *order)
}
