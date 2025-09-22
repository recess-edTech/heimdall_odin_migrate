
import dotenv from 'dotenv';
dotenv.config();

export const heimdallConfig = {
    DATABASE_URL: process.env.HEIMDALL_DATABASE_URL,
    DATABASE_PORT: process.env.HEIMDALL_DATABASE_PORT,
    DATABASE_USER: process.env.HEIMDALL_DATABASE_USER,
    DATABASE_PASSWORD: process.env.HEIMDALL_DATABASE_PASSWORD,
}


export const odinConfig = {
    DATABASE_URL: process.env.ODIN_DATABASE_URL,
    DATABASE_PORT: process.env.ODIN_DATABASE_PORT,
    DATABASE_USER: process.env.ODIN_DATABASE_USER,
    DATABASE_PASSWORD: process.env.ODIN_DATABASE_PASSWORD,
}



export const heimdallPostgresUrl = process.env.HEIMDALL_DATABASE_URL
export const odinPostgresUrl = process.env.ODIN_DATABASE_URL