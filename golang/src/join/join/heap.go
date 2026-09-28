package join

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Heap struct {
	items []fruititem.FruitItem
}

func NewHeap() *Heap {
	return &Heap{
		items: make([]fruititem.FruitItem, 0),
	}
}

func (h *Heap) IsEmpty() bool {
	return len(h.items) == 0
}

func (h *Heap) Push(value fruititem.FruitItem) {
	h.items = append(h.items, value)
	h.heapifyUp(len(h.items) - 1)
}

func (h *Heap) Pop() (fruititem.FruitItem, error) {
	if len(h.items) == 0 {
		return fruititem.FruitItem{}, errors.New("heap is empty")
	}

	root := h.items[0]

	lastIndex := len(h.items) - 1
	h.items[0] = h.items[lastIndex]
	h.items = h.items[:lastIndex]

	if len(h.items) > 0 {
		h.heapifyDown(0)
	}

	return root, nil
}

func (h *Heap) heapifyUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2

		if !h.items[parent].Less(h.items[index]) {
			break
		}

		h.items[index], h.items[parent] =
			h.items[parent], h.items[index]

		index = parent
	}
}

func (h *Heap) heapifyDown(index int) {
	for {
		left := 2*index + 1
		right := 2*index + 2
		largest := index

		if left < len(h.items) &&
			h.items[largest].Less(h.items[left]) {
			largest = left
		}

		if right < len(h.items) &&
			h.items[largest].Less(h.items[right]) {
			largest = right
		}

		if largest == index {
			break
		}

		h.items[index], h.items[largest] =
			h.items[largest], h.items[index]

		index = largest
	}
}
