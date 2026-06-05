const express = require('express');
const ordersController = require('../controllers/ordersController');

const router = express.Router();

router.get('/', ordersController.getAllOrders);

module.exports = router;
