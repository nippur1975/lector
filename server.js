const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// --- 1. CONFIGURACIÓN AZURE SQL ---
const dbConfig = {
    user: 'svc_writer_ep',
    password: 'vSkrQ4okJvAvobye',
    server: 'srv-db-east-us-estabilidadep.database.windows.net',
    database: 'db_estabilidadep_prd',
    requestTimeout: 90000, // SUBIR A 90000 (90 segundos) por seguridad
    options: {
        encrypt: true,
        trustServerCertificate: false,
        requestTimeout: 90000 // SUBIR A 90000 AQUÍ TAMBIÉN
    }
};

async function getConnection() {
    try {
        if (sql.globalConnection && sql.globalConnection.connected) {
            return sql.globalConnection;
        }
        const pool = await sql.connect(dbConfig);
        sql.globalConnection = pool;
        return pool;
    } catch (err) {
        console.error("❌ Error de conexión SQL:", err);
        throw err;
    }
}

// ==========================================
//   SISTEMA DE CACHÉ EN MEMORIA
// ==========================================

let cacheFlota = []; 

async function actualizarCacheFlota() {
    try {
        const pool = await getConnection();
        const request = pool.request(); 
        
        // CORRECCIÓN: Para el "Estado Actual" (Live), necesitamos mirar más atrás
        // para detectar barcos desconectados hace tiempo (como el T412 de hace 5 días).
        // Usamos -10 días para asegurarnos de verlo.
        const query = `
            WITH RankedData AS (
                SELECT 
                    ShipID as ship_id,
                    Pitch as pitch, 
                    Roll as roll, 
                    CalaID as cala_id,
                    Rumbo as heading,
                    Velocidad as speed,
                    Latitud as lat, 
                    Longitud as lon,
                    Date_event as date_event,
                    TipoAlarma,
                    EstadoAlarma,
                    ValorActual,
                    ROW_NUMBER() OVER (PARTITION BY ShipID ORDER BY Date_event DESC) as rn
                FROM [dbo].[Sensor_Flota]
                WHERE Date_event >= DATEADD(day, -10, GETDATE())  -- <--- CAMBIAR ESTE NÚMERO
            )
            SELECT * FROM RankedData WHERE rn = 1
        `;

        const result = await request.query(query);
        // ... (resto del código igual)
        
        const flotaProcesada = result.recordset.map(barco => {
            let estado = barco.EstadoAlarma;
            let tipo = barco.TipoAlarma;
            let valor = barco.ValorActual;

            if (!estado || estado === 'N/A' || estado === null) {
                if (Math.abs(barco.pitch) > 10) {
                    estado = "ACTIVADA";
                    tipo = barco.pitch > 0 ? "PITCH_SENTADO" : "PITCH_ENCABUZADO";
                    valor = barco.pitch;
                } else if (Math.abs(barco.roll) > 12) {
                    estado = "ACTIVADA";
                    tipo = barco.roll < 0 ? "ROLL_BABOR" : "ROLL_ESTRIBOR";
                    valor = barco.roll;
                } else {
                    estado = "DESACTIVADA";
                }
            }

            return {
                ...barco,
                EstadoAlarma: estado,
                TipoAlarma: tipo,
                ValorActual: valor,
                cala_id: parseInt(barco.cala_id) || 0 
            };
        });

        cacheFlota = flotaProcesada;
        // console.log(`🔄 Caché actualizada: ${cacheFlota.length} barcos (ventana 15 días)`);

    } catch (err) {
        console.error("❌ Error actualizando caché de flota:", err.message);
    }
}

// Iniciamos el ciclo de actualización
loopActualizacion();

async function loopActualizacion() {
    console.log("⏳ Iniciando actualización de caché de flota...");
    const inicio = Date.now();
    
    await actualizarCacheFlota(); // Espera a que termine la consulta pesada
    
    const fin = Date.now();
    const duracion = (fin - inicio) / 1000;
    console.log(`✅ Actualización completada en ${duracion} segundos.`);

    // Espera 10 segundos ANTES de volver a intentar
    // Esto da un respiro a la base de datos
    setTimeout(loopActualizacion, 10000); 
}

// --- 2. ENDPOINTS API ---

app.get('/api/ships', async (req, res) => {
    try {
        const pool = await getConnection();
        const result = await pool.request().query`
            SELECT DISTINCT ShipID as ship_id 
            FROM [dbo].[Sensor_Flota] 
            ORDER BY ShipID ASC
        `;
        res.json(result.recordset);
    } catch (err) {
        res.status(500).send(err.message);
    }
});

app.get('/api/datos', async (req, res) => {
    const { shipId, startDate, endDate } = req.query;
    if (!shipId) return res.status(400).send("Falta shipId");

    try {
        const pool = await getConnection();
        const request = pool.request();
        request.input('shipIdInput', sql.VarChar, shipId);

        let query = "";

        // CASO A: SI HAY FECHAS (Filtro específico)
        if (startDate && endDate) {
            request.input('start', sql.VarChar, startDate);
            request.input('end', sql.VarChar, endDate);
            
            query = `
                SELECT 
                    Date_event as date_event, 
                    Pitch as pitch, 
                    Roll as roll, 
                    CalaID as cala_id, 
                    Rumbo as heading, 
                    Velocidad as speed,
                    Latitud as lat, 
                    Longitud as lon,
                    ShipID as ship_id,
                    TipoAlarma, EstadoAlarma, ValorActual
                FROM [dbo].[Sensor_Flota] 
                WHERE ShipID = @shipIdInput
                AND Date_event >= CAST(@start AS DATE) 
                AND Date_event < DATEADD(day, 1, CAST(@end AS DATE))
                ORDER BY Date_event ASC
            `;
        } 
        // CASO B: NO HAY FECHAS (Carga inicial / Default)
        // CORRECCIÓN CLAVE: Usamos TOP 2500 ordenado DESC para obtener lo ÚLTIMO que haya, 
        // sin importar si fue ayer o hace 20 días.
        else {
            query = `
                SELECT * FROM (
                    SELECT TOP 2500
                        Date_event as date_event, 
                        Pitch as pitch, 
                        Roll as roll, 
                        CalaID as cala_id, 
                        Rumbo as heading, 
                        Velocidad as speed,
                        Latitud as lat, 
                        Longitud as lon,
                        ShipID as ship_id,
                        TipoAlarma, EstadoAlarma, ValorActual
                    FROM [dbo].[Sensor_Flota] 
                    WHERE ShipID = @shipIdInput
                    ORDER BY Date_event DESC
                ) AS sub
                ORDER BY sub.date_event ASC
            `;
        }

        const result = await request.query(query);
        res.json(result.recordset);

    } catch (err) {
        console.error(`Error historial (${shipId}):`, err.message);
        res.status(500).send(err.message);
    }
});

app.get('/api/flota/live', (req, res) => {
    res.json(cacheFlota);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Servidor API TASA V4.3 (Fix Barcos Offline) listo en http://localhost:${PORT}`);
});