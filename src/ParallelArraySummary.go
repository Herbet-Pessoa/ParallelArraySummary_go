package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

type Object struct {
	ID    int
	Total float64
	Grupo int
}

type ParallelArraySummary struct {
    Totals    []float64
    Grupos    []int
}


type partialResult struct {
	localTotalSum             float64
	localGroupSum             map[int]float64
	localLessThanFiveCount    int
	localGreaterOrEqualsCount int
}

func (pas *ParallelArraySummary) Carregamento(N int) {
    nElements := int(math.Pow10(N))

	var wg sync.WaitGroup

	// Define channels para mandar os resultados
    totalsChannel := make(chan float64, nElements)
    gruposChannel := make(chan int, nElements)

	// função para preencher 
	fillElement := func(i int) {
        total := rand.Float64() * 10
        grupo := rand.Intn(5) + 1

        totalsChannel <- total
        gruposChannel <- grupo

		wg.Done()
    }

	// Start goroutines para preencher em paralelo
    for i := 0; i < nElements; i++ {
		wg.Add(1)
        go fillElement(i)
    }

	// Esperar todas as goroutines terminarem de preencher
    wg.Wait()

    pas.Totals = make([]float64, nElements)
    pas.Grupos = make([]int, nElements)

    for i := 0; i < nElements; i++ {
        pas.Totals[i] = <-totalsChannel
        pas.Grupos[i] = <-gruposChannel
    }

	// Fechando os canais
    close(totalsChannel)
    close(gruposChannel)
}



func CreateFileName(N int, T int) string {
	return fmt.Sprintf("n%d_t%d", N, T)
}

func CreateOutputFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName + ".txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}

func RedirectOutputToFile(file *os.File) {
	os.Stdout = file
}

func CloseAndSaveFile(file *os.File) {
	// Retorna a saída padrão ao console
	os.Stdout = os.NewFile(1, "/dev/stdout")
	// Fecha o arquivo
	file.Close()
}



func (pas *ParallelArraySummary) Processamento(T int) (int64, error) {
	startTime := time.Now()

	var (
		totalSum                 float64
		groupSum                 map[int]float64
		lessThanFiveCount        int
		greaterOrEqualsCount     int
		mutex                    sync.Mutex
		wg                       sync.WaitGroup
	)

	groupSum = make(map[int]float64)

	// Inicializa groupSum com alocação inicial do slice
	for i := 1; i <= 5; i++ {
		groupSum[i] = 0
	}

	// Canal bufferizado para enviar resultados parciais
	resultChannel := make(chan partialResult, T)

	// Define a função de processamento
	processFunction := func(start, end int, resultChannel chan<- partialResult) {
		defer wg.Done()

		localTotalSum := 0.0
		localGroupSum := make(map[int]float64)
		localLessThanFiveCount := 0
		localGreaterOrEqualsCount := 0

		for i := start; i < end; i++ {
            total := pas.Totals[i]
            grupo := pas.Grupos[i]

            localTotalSum += total
            localGroupSum[grupo] += total

            if total < 5 {
                localLessThanFiveCount++
            } else {
                localGreaterOrEqualsCount++
            }
		}

		// Envia resultados parciais para o canal
		resultChannel <- partialResult{
			localTotalSum:             localTotalSum,
			localGroupSum:             localGroupSum,
			localLessThanFiveCount:    localLessThanFiveCount,
			localGreaterOrEqualsCount: localGreaterOrEqualsCount,
		}
	}

	// Divide o trabalho entre as threads
	numObjects := len(pas.Totals)
	chunkSize := numObjects / T
	for i := 0; i < T; i++ {
		wg.Add(1)
		start := i * chunkSize
		end := start + chunkSize
		if i == T-1 {
			end = numObjects
		}
		go processFunction(start, end, resultChannel)
	}

	// fecha o canal após processamento
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	// Processa resultados parciais do canal
	for result := range resultChannel {
		mutex.Lock()
		totalSum += result.localTotalSum
		for group, sum := range result.localGroupSum {
			groupSum[group] += sum
		}
		lessThanFiveCount += result.localLessThanFiveCount
		greaterOrEqualsCount += result.localGreaterOrEqualsCount
		mutex.Unlock()
	}

	// Calcula o tempo decorrido
	elapsedTime := time.Since(startTime).Nanoseconds()

	// Imprime os resultados
	fmt.Printf("Total Sum: %.2f\n", totalSum)
	fmt.Println("Group Sums:")
	for group, sum := range groupSum {
		fmt.Printf("Grupo %d: %.2f\n", group, sum)
	}
	fmt.Printf("Número de IDs com total < 5: %d\n", lessThanFiveCount)
	fmt.Printf("Número de IDs com total >= 5: %d\n", greaterOrEqualsCount)


	return elapsedTime, nil
}

func main() {
	var pas ParallelArraySummary

	var wg sync.WaitGroup

	// testes com diferentes valores de N e T
	for _, N := range []int{5, 7, 9} {
	// for _, N := range []int{5, 7/*, 9*/} {
		for _, T := range []int{1, 4, 16, 64, 256} {
			pas.Carregamento(N)

			file, err := CreateOutputFile(CreateFileName(N, T))
			if err != nil {
				fmt.Println("Erro ao criar o arquivo de saída:", err)
				return
			}
			defer CloseAndSaveFile(file)

			RedirectOutputToFile(file)

			fmt.Printf("Teste com N=%d, T=%d:\n", N, T)

			elapsedTime, err := pas.Processamento(T)
			if err != nil {
				fmt.Println("Erro ao processar objetos:", err)
				return
			}

			output := fmt.Sprintf("Tempo de processamento: %d nanossegundos\n------\n", elapsedTime)
			fmt.Print(output)
		}
	}

	wg.Wait()
}
