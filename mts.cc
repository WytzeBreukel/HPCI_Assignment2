#include <chrono>

#include <cstdio>

#include "matrix.h"
#include "mpi.h"
#include <bits/stdc++.h>
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

static int nieghbours[max_n_rows];
queue<int> to_visit;
static bool visited_nodes[max_n_rows];



double retrive_value(int row, int column){
   for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      return values[idx];
    }
  }
  return 0; 
}

void breath_first_search(int starting_node){
  // fprintf(stderr,"Rec start end %d \n",starting_node);
  visited_nodes[starting_node] = true;


  int nieghbours_found = 0;
  for(int j = 0; j< n_rows; j++){
    if (retrive_value(starting_node,j) != 0){
        if(!visited_nodes[j]){
          nieghbours[nieghbours_found] = j;
          nieghbours_found += 1;
        }
        
    }
  }
  for(int i = 0; i< nieghbours_found; i++){
    fprintf(stderr,"Found %d \n",nieghbours[i]);
  }
  for(int i = 0; i< nieghbours_found; i++){
    // fprintf(stderr, "Sanity check\n");
    // fprintf(stderr,"Rec start acces %d %d \n",i,nieghbours[i]);
    breath_first_search(nieghbours[i]);
    
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
  breath_first_search(0);
  

  return 0;
}
