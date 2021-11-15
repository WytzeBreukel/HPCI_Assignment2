#include <chrono>
#include<tuple>
#include <cstdio>
#include <queue>
#include "matrix.h"
#include "mpi.h"
using namespace std;
/* Global variables holding the matrix data. To complete this assignment
 * you are requested to only use arrays and access these arrays with
 * subscripts. Do not use pointers.
 */

const int max_n_elements = 214748364;
// const int max_n_elements = 131072;
const int max_n_rows = 16384;
int nnz, n_rows, n_cols;
static int values[max_n_elements];

static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];

static int merged_to[max_n_rows];
static tuple<int,int> merge_queue[max_n_rows];
int amount_of_merges = 0;
int total_weight = 0;
// static bool touched[max_n_rows];

//DONT forget to set this!
int last_row_ptr_index = -1;

const int max_int = 2147483647;

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

void clear_merge_queue(){
  std::fill(merge_queue, merge_queue+amount_of_merges, make_tuple(0,0));
}

void get_neighbours(int node){
 fprintf(stderr, "FOR %d \n",node);
  for(int k = 0; k< n_rows;k++){
    fprintf(stderr, "%d %d \n",k, retrive_value(node,k));
  }
}
void merge_nodes(int i, int j){
  
  // fprintf(stderr, "mimimum spanning tree part : modified %d %d \n", i, j);
  merged_to[j] = i;
  for(int k = 0; k < n_rows; k++){
    if(k == j || k == i){
      insert_value(i,k,max_int);
      continue;
    }
    int node_i_value = retrive_value(i,k); 
    int node_j_value = retrive_value(j,k); 
    if( node_i_value != max_int){
      // 8              9999                0
      if(node_i_value > node_j_value && node_j_value !=0 ){
      // fprintf(stderr, "insreting A %d for %d %d ivalue %d\n",node_j_value,i ,k,node_i_value);
      insert_value(i,k,node_j_value);
      }
      else if(node_i_value == 0 || node_j_value == max_int) {
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
void add_to_merge_queue(tuple<int,int> merge){
  for(int i = 0; i< amount_of_merges; i++){
    if (merge_queue[i] == merge)
    {
      // fprintf(stderr, "Already in here\n");
      return;
    }
  }
  merge_queue[amount_of_merges] = merge;
  amount_of_merges += 1;

}
void check_merge_queue(){
  fprintf(stderr,"MERGE QUEUE \n");
  for(int i = 0; i < amount_of_merges; i++){
    fprintf(stderr, " %d %d \n", get<0>(merge_queue[i]) , get<1>(merge_queue[i]));
  }
}
void preform_merges(){
  for(int i =0; i< amount_of_merges; i++){
    int node_i = get<0>(merge_queue[i]);
    int node_j = get<1>(merge_queue[i]);
    fprintf(stderr, "in mst %d %d \n", node_i, node_j);
    total_weight = total_weight + retrive_value(node_i,node_j);
    merge_nodes(merged_to[node_i], node_j);
  }

  // for(int i =0; i<amount_of_merges; i++){
  //   if(merged_to[i] == i){
  //     get_neighbours(i);
  //   }
  // }
}
void boruvka(){
  set_up_merged();
  
  while(true){
    amount_of_merges = 0;
    clear_merge_queue();
    for(int i = 0; i < n_rows; i++){
      
    //Gives problems if things are super huge more than maxint on non connected
    // fprintf(stderr, "\n\n\nHandleing %d\n",i);
    if(merged_to[i] == i){
      int lowest_weight = max_int;
      int lowest_node = -1;
      for(int j = 0; j<n_rows; j++){
          int weight = retrive_value(i,j);
          //last one prob uneccary
          if(weight != 0 && weight < lowest_weight && weight != max_int){
            lowest_weight = weight;
            lowest_node = j;
          }
        }
      if(lowest_node == -1){
        fprintf(stderr,"Unconnected or Done Total weight: %d \n", total_weight);
        return;
      }
      // if(merged_to[i] != lowest_node){
      //   fprintf(stderr, "lowest node %d value %d \n",lowest_node, lowest_weight);
      //   fprintf(stderr, "mimimum spanning tree part : unmodified %d %d \n", i, lowest_node);
      //   merge_nodes(merged_to[i],lowest_node);
      
      // }else{
      //   fprintf(stderr, "Skipping because %d  is already merged with %d \n",i,lowest_node);
      // }
      tuple<int,int> merge;
      if(i > lowest_node){
        merge =  make_tuple(lowest_node,i);
      }else{
        merge =  make_tuple(i, lowest_node);
      }
      add_to_merge_queue(merge);
      
  }else{
    // fprintf(stderr, "Already skipping MERGED %d \n",i);
  }
  }
  // check_merge_queue();
  preform_merges();

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
  auto start_time = std::chrono::high_resolution_clock::now();
  boruvka();
  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_time = end_time - start_time;
  fprintf(stdout, "%.20f\n", elapsed_time.count());

  return 0;
}
