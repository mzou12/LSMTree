#pragma once
#include <iostream>
#include <vector>
#include <random>
#include <optional>

#include "struct.hpp"

template<typename K, typename T>
class SkipList{

private:

    int max_level;
    int current_max;
    uint64_t _size = 0;
    templatedb::Node<K, T> *root;

    float prob;
    std::mt19937 rng;
    std::uniform_real_distribution<double> dist;

    int random_level() {
        int lvl = 1;
        while (dist(rng) < prob && lvl < max_level) {
            lvl++;
        }
        return lvl;
    }

    templatedb::Node<K,T>* find_prev_node_in_level(templatedb::Node<K,T> *prev, int lvl, const K& compare_value){
        templatedb::Node<K,T>* cur = prev;
        while (cur->forward[lvl] && cur->forward[lvl]->key < compare_value){
            cur = cur->forward[lvl];
        }
        return cur;
    }
    

public:

    SkipList(int this_max_level = 24, float this_prob = 0.5):
        max_level(this_max_level), prob(this_prob), current_max(1), rng(std::random_device{}()), dist(0.0, 1.0) 
    {
        root = new templatedb::Node<K,T>(max_level);
    }

    ~SkipList() {
        templatedb::Node<K,T>* node = root;
        while (node) {
            templatedb::Node<K, T>* next = node->forward[0];
            delete node;
            node = next;
        }
        root = nullptr;
    }

    void insert(const K& key, const T& new_value){
        int level = random_level();
        templatedb::Node<K, T> *node = root;
        templatedb::Node<K, T> *new_node = new templatedb::Node<K, T>(key, new_value, level);
        if (level > current_max){
            current_max = level;
        }
        std::vector<templatedb::Node<K, T>*> update(current_max, nullptr);
        for (int i = current_max - 1; i >= 0; i--){
            node = find_prev_node_in_level(node, i, key);
            update[i] = node;
        }
        for (int i = 0; i < level; ++i) {
            new_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = new_node;
        }

        if (new_node->forward[0] && new_node->forward[0]->key == key){
            templatedb::Node<K, T> *old_node = new_node->forward[0];
            if (old_node->value > new_value){
                new_node->value = old_node->value; // keep newest value
            }
            int old_level_size = old_node->forward.size();
            for (int i = 0; i < old_level_size; i++){
                if (i < new_node->forward.size()) {
                    new_node->forward[i] = old_node->forward[i];
                } else {
                    new_node->forward.push_back(old_node->forward[i]);
                    update[i]->forward[i] = new_node;
                }
            }
            delete old_node;
            return;
        }
        _size++;
    }

    std::optional<T> find(const K& key){
        templatedb::Node<K, T> *node = root;
        for (int i = current_max - 1; i >=0; i--){
            node = find_prev_node_in_level(node, i, key);
            if (node->forward[i] && node->forward[i]->key == key){
                return node->forward[i]->value;
            }
        }
        return std::nullopt;
    }

    T at(int key){
        if (key >= _size || key < 0){
            throw std::out_of_range("Index out of range");
        }
        templatedb::Node<K, T> *node = root;
        for (int i = 0; i <= key; i++){
            node = node->forward[0];
        }
        return node->value;
    }

    bool contain(const K& key){
        templatedb::Node<K, T> *node = root;
        for (int i = current_max-1; i>=0; i--){
            node = find_prev_node_in_level(node, i, key);
            if (node->forward[i] && node->forward[i]->key == key){
                return true;
            }
        }
        return false;
    }

    void erase_by_index(int key){
        if (key >= _size || key < 0){
            throw std::out_of_range("Index out of range");
        }
        templatedb::Node<K, T> *delete_one = root;
        for (int i = 0; i <= key; i++){
            delete_one = delete_one->forward[0];
        }
        templatedb::Node<K, T> *node = root;
        // std::vector<templatedb::Node<T>*> update(delete_one->forward.size()); 
        for(int i = current_max - 1; i>=0; i--){
            node = find_prev_node_in_level(node, i, delete_one->key);
            if (node->forward[i] && node->forward[i] == delete_one){
                node->forward[i] = delete_one->forward[i];
            }
        }
        while (current_max > 1 && !root->forward[current_max - 1]){
            current_max -= 1;
        }
        delete delete_one;
        _size--;
    }

    void erase_by_key(const K& key){
        templatedb::Node<K, T> *delete_one = root;
        std::vector<templatedb::Node<K, T>*> update(current_max, nullptr);
        for (int i = current_max - 1; i>=0; i--){
            delete_one = find_prev_node_in_level(delete_one, i, key);
            update[i] = delete_one;
        }
        if (delete_one->forward[0] && delete_one->forward[0]->key == key){
            for (int i = 0; i < delete_one->forward.size(); i++){
                update[i]->forward[i] = delete_one->forward[i];
            }
            while (current_max > 1 && !root->forward[current_max - 1]){
                current_max -= 1;
            }
            delete delete_one;
            _size--;
        }

    }

    int size() const{
        return _size;
    }

    bool empty() const{
        return _size == 0;
    }

    T& operator[](int index) {
        templatedb::Node<K, T>* node = root->forward[0];
        int count = 0;
        while (node) {
            if (count == index) return node->value;
            node = node->forward[0];
            count++;
        }
        throw std::out_of_range("Index out of range");
    }

    const T& operator[](int index) const {
        templatedb::Node<K, T>* node = root->forward[0];
        int count = 0;
        while (node) {
            if (count == index) return node->value;
            node = node->forward[0];
            count++;
        }
        throw std::out_of_range("Index out of range");
    }

    void print() const {
        std::cout << "\n===== SkipList Structure =====\n";
        for (int lvl = current_max - 1; lvl >= 0; --lvl) {
            templatedb::Node<K, T>* node = root->forward[lvl];
            std::cout << "Level " << lvl << ": ";
            while (node) {
                std::cout << node->key << " -> ";
                node = node->forward[lvl];
            }
            std::cout << "nullptr\n";
        }
        std::cout << "==============================\n";
    }

    void clear(){
        templatedb::Node<K, T>* node = root->forward[0];
        while (node) {
            templatedb::Node<K, T>* next = node->forward[0];
            delete node;
            node = next;
        }
    
        for (auto& ptr : root->forward) {
            ptr = nullptr;
        }
    
        _size = 0;
        current_max = 1;
    }

    class Iterator{
        private: 
            templatedb::Node<K, T>* current;
        
        public:
            Iterator() : current(nullptr) {}

            Iterator(templatedb::Node<K, T>* node) : current(node) {}

            T& operator*() { return current->value; }
            const T& operator*() const { return current->value; }
        
            Iterator& operator++() {
                current = current ? current->forward[0] : nullptr;
                return *this;
            }
            T* operator->() { return &(current->value); } 
            const T* operator->() const { return &(current->value); }
        
            bool operator!=(const Iterator& other) const {
                return current != other.current;
            }

            bool hasNext() const {
                return current != nullptr;
            }
        
            T& next() {
                if (!current) {
                    throw std::out_of_range("Cannot call next() at end!");
                }
                T& val = current->value;
                current = current->forward[0];
                return val;
            }

    };

    Iterator begin() {
        return Iterator(root->forward[0]);
    }

    Iterator end() {
        return Iterator(nullptr);
    }

    Iterator begin() const {
        return Iterator(root->forward[0]);
    }

    Iterator end() const {
        return Iterator(nullptr);
    }
    
};