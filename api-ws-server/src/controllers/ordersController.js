const ordersService = require('../services/ordersService');

class OrdersController {
  async getAllOrders(req, res, next) {
    try {
      const orders = await ordersService.getAllOrders();
      res.status(200).json({
        status: 'success',
        results: orders.length,
        data: { orders },
      });
    } catch (err) {
      next(err);
    }
  }
}

module.exports = new OrdersController();
