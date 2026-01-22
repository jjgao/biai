import { Router } from 'express'
import { getTablesList } from '../services/queryService.js'

const router = Router()

router.get('/tables', async (_req, res) => {
  try {
    const tables = await getTablesList()
    return res.json({ success: true, data: tables })
  } catch (error) {
    return res.status(500).json({ success: false, error: 'Failed to fetch tables' })
  }
})

export default router
