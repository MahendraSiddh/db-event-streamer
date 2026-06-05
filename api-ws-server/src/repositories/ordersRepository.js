const db = require('../db/pool');

class OrdersRepository {
  // Fetch all orders sorted by most recently updated
  async findAll() {
    const query = 'SELECT id, customer_name, product_name, status, updated_at FROM orders ORDER BY updated_at DESC';
    const { rows } = await db.query(query);
    return rows;
  }
}

module.exports = new OrdersRepository();
