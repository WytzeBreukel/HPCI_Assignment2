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
static int values[max_n_elements];

static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];

static int merged_to[max_n_rows];
// static bool touched[max_n_rows];

//DONT forget to set this!
int last_row_ptr_index = -1;

int max_int = 2147483647;

void status_update(){


  for(int i =0; i < n_rows; i++){
     fprintf(stderr, "index %d start %d end %d \n", i, row_ptr_begin[i] , row_ptr_end[i]);
     for(int k = row_ptr_begin[i]; k <= row_ptr_end[i]; k++){
       fprintf(stderr, " collum %d value %d \n",  col_ind[k], values[k]);
     }
  }

  fprintf(stderr, "last status %d \n", last_row_ptr_index);
}

int retrive_value(int row, int column){
   for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      return values[idx];
    }
  }
  return 0; 
}

void insert_value(int row, int column, int value){
  // fprintf(stderr,"Inserting %d at %d %d\n", value,row,column);
  for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      // fprintf(stderr, "%d %d exists \n",row,column);
      values[idx] = value;
      return;
    }
  }
 
  if(value == 0){
    return;
  }

  int new_begin_ptr = row_ptr_end[last_row_ptr_index]+1;
  // fprintf(stderr,"New begin pointer  %d\n", new_begin_ptr);
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

void get_neighbours(int node){
 fprintf(stderr, "FOR %d \n",node);
  for(int k = 0; k< n_rows;k++){
    fprintf(stderr, "%d %d \n",k, retrive_value(node,k));
  }
}
void merge_nodes(int i, int j){
  
  fprintf(stderr, "mimimum spanning tree part : modified %d %d \n", i, j);
  // get_neighbours(i);
  // get_neighbours(j);
  merged_to[j] = i;
  for(int k = 0; k < n_rows; k++){
    if(k == j || k == i){
      insert_value(i,k,max_int);
      continue;
    }
    int node_i_value = retrive_value(i,k); 
    int node_j_value = retrive_value(j,k); 
    if( node_i_value != max_int){
      //xor 8              0                0
      if(node_i_value > node_j_value && node_j_value !=0 ){
      // fprintf(stderr, "insreting A %d for %d %d ivalue %d\n",node_j_value,i ,k,node_i_value);
      insert_value(i,k,node_j_value);
      }
      else if(node_i_value == 0) {
        // fprintf(stderr, "insreting %d for %d %d ivalue %d\n",node_j_value,i ,k,node_i_value);
        insert_value(i,k,node_j_value);
      }
    
    };
 

  }
}
void set_up_merged(){
  for(int i = 0; i<n_rows; i++){
    merged_to[i] = i;
  }
}

void boruvka(){
  set_up_merged();
  for(int i = 0; i < n_rows; i++){
    //Gives problems if things are super huge more than maxint on non connected
    fprintf(stderr, "\n\n\n\n\n\n\nHandleing %d\n",i);
    if(merged_to[i] == i){
      int lowest_weight = max_int;
      int lowest_node = -1;
      for(int j = 0; j<n_rows; j++){
          int weight =  retrive_value(i,j);
          //last one prob uneccary
          if(weight != 0 && weight < lowest_weight && weight != max_int){
            lowest_weight = weight;
            lowest_node = j;
          }
        }
      if(lowest_node == -1){
        fprintf(stderr,"Unconnected\n");
        throw;
      }
      if(merged_to[i] != lowest_node){
        fprintf(stderr, "lowest node %d value %d \n",lowest_node, lowest_weight);
        fprintf(stderr, "mimimum spanning tree part : unmodified %d %d \n", i, lowest_node);
        merge_nodes(merged_to[i],lowest_node);
      
      }else{
        fprintf(stderr, "Skipping because %d  is already merged with %d \n",i,lowest_node);
      }
  
  }else{
    fprintf(stderr, "Already skipping MERGED %d \n",i);
  }
   if(i == 7){
     get_neighbours(i);
     throw;
  }
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
  last_row_ptr_index = n_rows - 1;
  if (!ok)
    {
      fprintf(stderr, "failed to load matrix.\n");
      return -1;
    }

  /* For debugging, can be removed when implementation is finished. */
  // dump_nonzeros(n_rows, values, col_ind, row_ptr_begin, row_ptr_end);

  // fprintf(stderr, " \n %d \n", retrive_value(1,2));

  boruvka();
  

  return 0;
}
