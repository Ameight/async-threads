package main

import (
	"fmt"
	"github.com/Ameight/async-threads"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

// Функция для визуализации графа
func visualizeGraph(root *async_process.Thread, graph *cgraph.Graph) {
	// Создайте узел для текущего Thread с указанием его действия
	node, _ := graph.CreateNode(root.Name)

	// Обходите зависимости текущего Thread и создайте связи
	for _, dep := range root.DependsOn {
		// Создайте узел для зависимости
		depNode, _ := graph.CreateNode(dep.Name)

		// Создайте ребро между текущим Thread и его зависимостью
		graph.CreateEdge(root.Name, node, depNode)

		// Рекурсивно обходите зависимости
		visualizeGraph(dep, graph)

		//depNode.Free()
	}
	//node.Free()
}

func main() {
	// Создайте конфигурацию Thread и их зависимости
	threads := make(map[string]*async_process.Thread)
	threads["thread1"] = &async_process.Thread{DependsOn: []*async_process.Thread{}}
	threads["thread2"] = &async_process.Thread{DependsOn: []*async_process.Thread{}}
	threads["thread3"] = &async_process.Thread{DependsOn: []*async_process.Thread{threads["thread1"]}}
	threads["thread4"] = &async_process.Thread{DependsOn: []*async_process.Thread{threads["thread1"], threads["thread2"], threads["thread3"]}}
	threads["thread5"] = &async_process.Thread{DependsOn: []*async_process.Thread{threads["thread1"], threads["thread2"], threads["thread3"], threads["thread4"]}}

	// Создайте объект Graphviz
	g := graphviz.New()
	defer g.Close()

	// Получите объект Graph
	graph, _ := g.Graph()

	// Визуализируйте граф, начиная с корневого Thread
	rootThread := threads["thread1"] // Измените на корневой Thread, который вам нужен
	visualizeGraph(rootThread, graph)

	// Создайте изображение (например, в формате PNG)
	_ = g.RenderFilename(graph, "png", "output.png")

	fmt.Println("Граф визуализирован и сохранен в output.png")
}
