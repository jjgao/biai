import express from 'express'
import cors from 'cors'
import dotenv from 'dotenv'
import apiRoutes from './routes/api.js'
import datasetsRoutes from './routes/datasets.js'
import databasesRoutes from './routes/databases.js'
import dashboardService from './services/dashboardService.js'
import logger from './utils/logger.js'
import { requestLogger } from './middleware/requestLogger.js'

dotenv.config()

const app = express()
const PORT = process.env.PORT || 5001

app.use(cors({
  exposedHeaders: ['X-Request-Id']
}))
app.use(express.json())
app.use(requestLogger)

app.use('/api', apiRoutes)
app.use('/api/datasets', datasetsRoutes)
app.use('/api/databases', databasesRoutes)

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', message: 'BIAI Server is running' })
})

// Initialize dashboard table
dashboardService.initializeTable().then(() => {
  logger.info('Dashboard table initialized')
}).catch(err => {
  logger.error('Failed to initialize dashboard table', err)
})

app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`)
})

