import postgres from 'postgres'
import { heimdallPostgresUrl, odinPostgresUrl, } from 'config/config'




export const heimdallSql = postgres(heimdallPostgresUrl, { max: 1 })
export const odinSql = postgres(odinPostgresUrl, { max: 1 })