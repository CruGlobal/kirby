'use strict'

module.exports = () => {
  // Use dotenv to load local development overrides
  require('dotenv').config()
  return {
    ENVIRONMENT: process.env.ENVIRONMENT || 'development',
    MASTER_PG_USER: process.env.MASTER_PG_USER || '',
    MASTER_PG_ADDR: process.env.MASTER_PG_ADDR || '',
    MASTER_PG_DB: process.env.MASTER_PG_DB || '',
    MASTER_PG_PASS: process.env.MASTER_PG_PASS || '',
    MASTER_PG_PORT: process.env.MASTER_PG_PORT || '',
    SLAVE_PG_USER: process.env.SLAVE_PG_USER || '',
    SLAVE_PG_ADDR: process.env.SLAVE_PG_ADDR || '',
    SLAVE_PG_DB: process.env.SLAVE_PG_DB || '',
    SLAVE_PG_PASS: process.env.SLAVE_PG_PASS || '',
    SLAVE_PG_PORT: process.env.SLAVE_PG_PORT || ''
  }
}









