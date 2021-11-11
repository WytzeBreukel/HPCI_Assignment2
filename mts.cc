#include <chrono>

#include <cstdio>
#include <queue>
#include "matrix.h"
#include "mpi.h"
using namespace std;
/* Global variables holding the matrix data. To complete this assignment
 * you are requested to only use arrays and access these arrays with
 * subscripts. Do not use pointers.
 */

const int max_n_elements = 131072;
const int max_n_rows = 16384;
int nnz, n_rows, n_cols;
static double values[max_n_elements];

static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];

static bool merged[max_n_rows];

int last_row_ptr_index = 0;


double retrive_value(int row, int column){
   for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      return values[idx];
    }
  }
  return 0; 
}

void insert_value(int row, int column, double value){
  for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      values[idx] = value;
      return;
    }
  }
 
  if(value == 0){
    return;
  }

  int new_begin_ptr = row_ptr_end[last_row_ptr_index]+1;

  if(last_row_ptr_index == row){

     new_begin_ptr = row_ptr_begin[last_row_ptr_index];
     int wanting_to_place_at = new_begin_ptr + (row_ptr_end[row]-row_ptr_begin[row]+1);
     if(wanting_to_place_at < max_n_elements){
    
        values[wanting_to_place_at] = value;
        row_ptr_end[row] = row_ptr_end[row]+1;
        col_ind[wanting_to_place_at] = column;
        return;
     }

    
  }

  if(new_begin_ptr + (row_ptr_end[row]-row_ptr_begin[row]+1) >= max_n_elements){
    fprintf(stderr, "Resuffle needed");
    throw "shuffle";
    // reshuffle();
    // insert_value(row,column,value);
   
    return;
  } 

  int counter = new_begin_ptr;
  for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    
    col_ind[counter] = col_ind[idx];
    values[counter] = values[idx];
    counter = counter + 1;
  }

  col_ind[counter] = column;
  values[counter] = value;

  row_ptr_begin[row] = new_begin_ptr;
  row_ptr_end[row] = counter;

  last_row_ptr_index = row;

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
void merge_nodes(int i, int j){
  fprintf(stderr, "mimimum spanning tree part : %d %d", i, j);
  

}

void boruvka(){
  for(int i = 0; i < n_rows; i++){
    //Gives problems if things are super huge on non connected
    int lowest_weight = 2147483647;
    int lowest_node = -1;
    for(int j = 0; j<n_rows; j++){
      if(!merged[j]){
        int weight =  retrive_value(i,j);
        if(weight != 0 && weight < lowest_weight){
          lowest_weight = weight;
          lowest_node = j;
        }
      }
    }
    if(lowest_node == -1){
      fprintf(stderr,"Unconnected");
      throw;
    }
    // fprintf(stderr, "lowest node %d value %d \n",lowest_node, lowest_weight);
    merge_nodes(i,lowest_node);
    
  }

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

  // fprintf(stderr, " \n %f \n", retrive_value(1,2));
  boruvka();
  

  return 0;
}
