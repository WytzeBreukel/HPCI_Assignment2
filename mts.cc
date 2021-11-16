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

// const int max_n_elements = 214748364;
const int max_n_elements = 131072;
const int max_n_rows = 16384;
int nnz, n_rows, n_cols;
static double values[max_n_elements];

static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];


struct Edge {
    int node_a;
    int node_b;
 
    double weight;
    Edge(int node_a, int node_b, double weight)
        : node_a(node_a), node_b(node_b), weight(weight)
         {
    }
};

// void status_update(){

//   for(int i =0; i < n_rows; i++){
//      fprintf(stderr, "index %d start %d end %d \n", i, row_ptr_begin[i] , row_ptr_end[i]);
//      for(int k = row_ptr_begin[i]; k <= row_ptr_end[i]; k++){
//        fprintf(stderr, " collum %d value %d orignal row %d \n",  col_ind[k], get<0>(values[k]), get<1>(values[k]));
//      }
//   }

//   fprintf(stderr, "last status %d \n", last_row_ptr_index);
// }

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


void get_neighbours(int node){
 fprintf(stderr, "FOR %d \n",node);
  for(int k = 0; k< n_rows;k++){
    fprintf(stderr, "%d %f \n",k, retrive_value(node,k));
  }
}
void boruvka(){
  fprintf(stderr, "Boruvka \n");
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
  // status_update();
  auto start_time = std::chrono::high_resolution_clock::now();
  boruvka();
  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_time = end_time - start_time;
  fprintf(stdout, "%.20f\n", elapsed_time.count());

  return 0;
}
