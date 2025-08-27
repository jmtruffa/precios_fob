package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

type PrecioFOB struct {
	Fecha    string   `json:"fecha"`
	Circular string   `json:"circular"`
	Posicion string   `json:"posicion"`
	Precio   *float64 `json:"precio"`
	MesDesde *int     `json:"mesDesde"`
	AnoDesde *int     `json:"añoDesde"`
	MesHasta *int     `json:"mesHasta"`
	AnoHasta *int     `json:"añoHasta"`
}

func main() {
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("Iniciando importación de precios FOB...")

	conn := connectToDB()
	defer conn.Close(context.Background())

	// Obtener última fecha registrada
	var lastDate *time.Time
	err := conn.QueryRow(context.Background(), `SELECT MAX(date) FROM precios_fob`).Scan(&lastDate)
	if err != nil {
		log.Fatalf("Error consultando última fecha: %v", err)
	}

	var startDate time.Time
	if lastDate == nil {
		startDate = time.Date(1993, 1, 4, 0, 0, 0, 0, time.UTC)
	} else {
		startDate = lastDate.AddDate(0, 0, 1)
	}

	today := time.Now()
	inserted := 0

	for d := startDate; !d.After(today); d = d.AddDate(0, 0, 1) {
		precios, err := fetchPreciosFOB(d, 3)
		if err != nil {
			log.Printf("Error consultando %s: %v", d.Format("2006-01-02"), err)
			continue
		}
		if len(precios) == 0 {
			continue
		}

		insertedThisDay := 0 // <<--- nuevo

		for _, p := range precios {
			if p.Precio == nil || p.MesDesde == nil || p.AnoDesde == nil || p.MesHasta == nil || p.AnoHasta == nil {
				log.Printf("Fila incompleta (precio o fecha NULL) para %s / %s. Omitida.", p.Fecha, p.Posicion)
				continue
			}

			parsedDate, err := time.Parse("2006-01-02 15:04:05.000", p.Fecha)
			if err != nil {
				log.Printf("Fecha malformateada: %s", p.Fecha)
				continue
			}

			var exists bool
			err = conn.QueryRow(context.Background(),
				`SELECT EXISTS(SELECT 1 FROM precios_fob WHERE date=$1 AND posicion=$2)`,
				parsedDate, p.Posicion).Scan(&exists)
			if err != nil {
				log.Printf("Error verificando duplicado: %v", err)
				continue
			}
			if exists {
				continue
			}

			_, err = conn.Exec(context.Background(), `
				INSERT INTO precios_fob 
				(date, circular, posicion, precio, mes_desde, ano_desde, mes_hasta, ano_hasta)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				parsedDate, p.Circular, p.Posicion, *p.Precio,
				*p.MesDesde, *p.AnoDesde, *p.MesHasta, *p.AnoHasta,
			)
			if err != nil {
				log.Printf("Error insertando fila: %v", err)
			} else {
				inserted++
				insertedThisDay++
			}
		}

		if insertedThisDay > 0 {
			fmt.Printf("Insertada fecha: %s\n", d.Format("2006-01-02"))
		}
	}
	fmt.Printf("Proceso completado. Filas insertadas: %d\n", inserted)
	fmt.Println("-------------------------------------------------------------")
}

func fetchPreciosFOB(date time.Time, retries int) ([]PrecioFOB, error) {
	url := fmt.Sprintf("https://magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=%s", date.Format("02/01/2006"))

	log.Printf("Consultando URL: %s", url)

	for i := 0; i <= retries; i++ {
		resp, err := http.Get(url)
		if err != nil {
			if i == retries {
				return nil, fmt.Errorf("fallo al conectar con la API: %w", err)
			}
			log.Printf("Reintento %d/%d: error de conexión, esperando %d segundos...", i+1, retries+1, 2*(i+1))
			time.Sleep(time.Second * time.Duration(2*(i+1)))
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			if i == retries {
				return nil, fmt.Errorf("API respondió con código: %d", resp.StatusCode)
			}
			log.Printf("Reintento %d/%d: API respondió con código %d, esperando %d segundos...", i+1, retries+1, resp.StatusCode, 2*(i+1))
			time.Sleep(time.Second * time.Duration(2*(i+1)))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error leyendo respuesta: %w", err)
		}

		// Log de la respuesta para debugging
		log.Printf("Respuesta del API (primeros 500 caracteres): %s", string(body[:min(len(body), 500)]))
		log.Printf("Longitud de la respuesta: %d bytes", len(body))
		log.Printf("Content-Type: %s", resp.Header.Get("Content-Type"))

		// Verificar si la respuesta está vacía
		if len(body) == 0 {
			if i == retries {
				return nil, fmt.Errorf("API devolvió respuesta vacía")
			}
			log.Printf("Reintento %d/%d: API devolvió respuesta vacía, esperando %d segundos...", i+1, retries+1, 2*(i+1))
			time.Sleep(time.Second * time.Duration(2*(i+1)))
			continue
		}

		// Verificar si la respuesta es HTML (error común)
		if len(body) > 0 && (body[0] == '<' || string(body[:5]) == "<html") {
			if i == retries {
				return nil, fmt.Errorf("API devolvió HTML en lugar de JSON: %s", string(body[:min(len(body), 200)]))
			}
			log.Printf("Reintento %d/%d: API devolvió HTML, esperando %d segundos...", i+1, retries+1, 2*(i+1))
			time.Sleep(time.Second * time.Duration(2*(i+1)))
			continue
		}

		// Verificar si la respuesta es un mensaje de error
		if len(body) > 0 && (body[0] == 'E' || string(body[:5]) == "Error") {
			if i == retries {
				return nil, fmt.Errorf("API devolvió mensaje de error: %s", string(body))
			}
			log.Printf("Reintento %d/%d: API devolvió error '%s', esperando %d segundos...", i+1, retries+1, string(body), 2*(i+1))
			time.Sleep(time.Second * time.Duration(2*(i+1)))
			continue
		}

		// Intentar decodificar como {"posts": [...]}
		var wrapper struct {
			Posts []PrecioFOB `json:"posts"`
		}
		if err := json.Unmarshal(body, &wrapper); err == nil {
			log.Printf("JSON parseado exitosamente como wrapper con %d posts", len(wrapper.Posts))
			return wrapper.Posts, nil
		}

		// Si falla, intentar como array plano
		var direct []PrecioFOB
		if err := json.Unmarshal(body, &direct); err == nil {
			log.Printf("JSON parseado exitosamente como array directo con %d elementos", len(direct))
			return direct, nil
		}

		// Si ambos fallan, mostrar el error específico del JSON
		log.Printf("Error parseando wrapper: %v", json.Unmarshal(body, &wrapper))
		log.Printf("Error parseando array directo: %v", json.Unmarshal(body, &direct))

		if i == retries {
			return nil, fmt.Errorf("error al parsear JSON: no se pudo interpretar como objeto ni como array")
		}

		log.Printf("Reintento %d/%d: JSON inválido, esperando %d segundos...", i+1, retries+1, 2*(i+1))
		time.Sleep(time.Second * time.Duration(2*(i+1)))
	}

	return nil, fmt.Errorf("fallo tras %d reintentos", retries)
}

// Función auxiliar para min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func connectToDB() *pgx.Conn {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbHost := os.Getenv("POSTGRES_HOST")
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")

	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("No se pudo conectar a la base de datos: %v", err)
	}
	return conn
}
