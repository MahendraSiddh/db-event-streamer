module.exports = (err, req, res, next) => {
  err.statusCode = err.statusCode || 500;
  err.status = err.status || 'error';

  if (err.isOperational) {
    res.status(err.statusCode).json({
      status: err.status,
      message: err.message,
    });
  } else {
    // Unhandled system or developer errors
    console.error('CRITICAL ERROR:', err.stack || err.message);
    res.status(500).json({
      status: 'error',
      message: 'An unexpected internal server error occurred',
    });
  }
};
