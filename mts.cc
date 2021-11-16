#include <chrono>
#include <tuple>
#include <cstdio>
#include <queue>
#include "matrix.h"
#include "mpi.h"
using namespace std;
/* Global variables holding the matrix data. To complete this assignment
 * you are requested to only use arrays and access these arrays with
 * subscripts. Do not use pointers.
 */

struct Edge {
    int node_a;
    int node_b;
 
    double weight;
    Edge(int node_a, int node_b, double weight)
        : node_a(node_a), node_b(node_b), weight(weight)
         {
    }
};

struct CompareWeight {
    bool operator()(Edge const& edge1, Edge const& edge2)
    {
        return edge1.weight > edge2.weight;
    }
};
// const int max_n_elements = 214748364;
const int max_n_elements = 131072;
// const int max_n_rows = 16384;
const int max_n_rows = 27993600;
int nnz, n_rows, n_cols;
static double values[max_n_elements];

static int node_location[max_n_rows];
static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

static priority_queue<Edge, vector<Edge>, CompareWeight> graph[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];

static double total_weight = 0;



// void status_update(){

//   for(int i =0; i < n_rows; i++){
//      fprintf(stderr, "index %d start %d end %d \n", i, row_ptr_begin[i] , row_ptr_end[i]);
//      for(int k = row_ptr_begin[i]; k <= row_ptr_end[i]; k++){
//        fprintf(stderr, " collum %d value %d orignal row %d \n",  col_ind[k], get<0>(values[k]), get<1>(values[k]));
//      }
//   }

//   fprintf(stderr, "last status %d \n", last_row_ptr_index);
// }
void print_edge(Edge edge){
  fprintf(stderr,"%d - %f - %d\n",edge.node_a, edge.weight, edge.node_b);
}

double retrive_value(int row, int column){
   for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      return values[idx];
    }
  }
  return 0; 
}

void breath_first_search(int starting_node){

  visited_nodes[starting_node] = true;

  to_visit.push(starting_node);
 
  while(!to_visit.empty()){
    for(int j = 0; j< n_rows; j++){
      if (retrive_value(starting_node,j) != 0){
          if(!visited_nodes[j]){
            to_visit.push(j);
            visited_nodes[j] = true;
          }
      }
    }
    fprintf(stderr,"%d \n",starting_node);
    to_visit.pop();
    starting_node = to_visit.front();
  }
}

void status_merging(){
  for(int i = 0; i <n_rows; i++){
    fprintf(stderr,"Node %d is in component %d \n",i, node_location[i]);
  }
}

void show_lowest_edge(){
  for(int i = 0; i< n_rows; i++){
    print_edge(graph[i].top());
  }
}
void show_edges(int node){
  while(!graph[node].empty()){
    print_edge(graph[node].top());
    graph[node].pop();
  }
  throw;
}
void get_neighbours(int node){
 fprintf(stderr, "FOR %d \n",node);
  for(int k = 0; k< n_rows;k++){
    fprintf(stderr, "%d %f \n",k, retrive_value(node,k));
  }
}
void create_structs(){
  for(int i = 0; i < n_rows; i++){
    for(int k = 0; k < n_rows; k++){
      double value = retrive_value(i,k);
      if(value != 0 ){
        graph[i].push(Edge(i,k,retrive_value(i,k)));
      }
    }
  }
}
void merge(int node_a, int node_b){
    fprintf(stderr, "In MST %d - %d \n",node_a, node_b);
    // if(graph[node_a].size() < graph[node_b].size()){
    //   swap(graph[node_a], graph[node_b]);
    // }
    while(!graph[node_b].empty()){
        graph[node_location[node_a]].push(graph[node_b].top());
        graph[node_b].pop();
    }
    node_location[node_b] = node_location[node_a];
  }
bool is_self_edge(int node_a, int node_b){
  return node_location[node_a] == node_location[node_b];
}
void setup_location_array(){
  for(int i = 0; i < n_rows; i++){
    node_location[i] = i;
  }
}
void boruvka(){
  fprintf(stderr, "Boruvka \n");
  setup_location_array();
  while(true){
    // print_edge(graph[0].top());
    Edge node_to_merge = graph[0].top();

    if(is_self_edge(0,node_to_merge.node_b)){
      // fprintf(stderr, "self edge\n");
      graph[0].pop();
    }else{
      merge(node_to_merge.node_a,node_to_merge.node_b);
      total_weight = total_weight + node_to_merge.weight;
    };
    if(graph[0].empty()){
      break;
    }
  }
  fprintf(stderr, "Total weigth: %f\n", total_weight);
}
int
main(int argc, char **argv)
{
  if (argc != 2)
    {
      fprintf(stderr, "usage: %s <filename>\n", argv[0]);
      return -1;
    }

  
  bool ok(false);

  ok = load_matrix_market(argv[1], max_n_elements, max_n_rows,
                          nnz, n_rows, n_cols,
                          values, col_ind, row_ptr_begin, row_ptr_end);
  if (!ok)
    {
      fprintf(stderr, "failed to load matrix.\n");
      return -1;
    }

  /* For debugging, can be removed when implementation is finished. */
  // dump_nonzeros(n_rows, values, col_ind, row_ptr_begin, row_ptr_end);

  fprintf(stderr, " \n %f \n", retrive_value(1,2));

  create_structs();
  // show_lowest_edge();
  // status_update();
  auto start_time = std::chrono::high_resolution_clock::now();
  boruvka();

  // show_edges(1);
 
  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_time = end_time - start_time;
  fprintf(stdout, "%.20f\n", elapsed_time.count());

  return 0;
}
