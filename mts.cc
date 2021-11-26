#include <chrono>
#include <tuple>
#include <cstdio>
#include <queue>
#include <vector>
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
const int max_n_elements = 214748368;
// const int max_n_elements = 131072;
// const int max_n_elements = 760648352;
// const int max_n_rows = 16384;
const int max_n_rows = 27993600;
int nnz, n_rows, n_cols;
static double values[max_n_elements];

static int node_location[max_n_rows];
static int col_ind[max_n_elements];
static int row_ptr_begin[max_n_rows];
static int row_ptr_end[max_n_rows];

static priority_queue<Edge, vector<Edge>, CompareWeight> graph[max_n_rows];

static vector <Edge> trees[max_n_rows];

queue<int> to_visit;
static bool visited_nodes[max_n_rows];

static double total_weight = 0;

static int node_ownership[max_n_rows];





void status_update(){

  for(int i =0; i < n_rows; i++){
     fprintf(stderr, "index %d start %d end %d \n", i, row_ptr_begin[i] , row_ptr_end[i]);
     for(int k = row_ptr_begin[i]; k <= row_ptr_end[i]; k++){
       fprintf(stderr, " collum %d value %f \n",  col_ind[k], values[k]);
     }
  }
}
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

void divide_nodes(int starting_node, int amount_of_process){
  int process_id = 0;
  int amount_of_nodes_per_process = n_rows/amount_of_process;

  int nodes_assigned = 0;

  visited_nodes[starting_node] = true;
  // fprintf(stderr," %d",starting_node);
  to_visit.push(starting_node);
  while(!to_visit.empty()){
    for(int idx = row_ptr_begin[starting_node]; idx<= row_ptr_end[starting_node]; idx++){
          if(!visited_nodes[col_ind[idx]]){
            // fprintf(stderr," %d",col_ind[idx]);  
            to_visit.push(col_ind[idx]);
            visited_nodes[col_ind[idx]] = true;


            node_ownership[col_ind[idx]] = process_id;
            nodes_assigned = nodes_assigned + 1;
            if(nodes_assigned >= amount_of_nodes_per_process){
              process_id = process_id + 1;
              nodes_assigned = 0;
            }
          }
    }
    // fprintf(stderr,"%d \n",starting_node);
    to_visit.pop();
    starting_node = to_visit.front();
    // fprintf(stderr,"\n");
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
    for(int idx = row_ptr_begin[i]; idx <= row_ptr_end[i]; idx++){
    
        graph[i].push(Edge(i,col_ind[idx],values[idx]));
        graph[col_ind[idx]].push(Edge(col_ind[idx], i, values[idx]));
      }
    }
  }
void show_node_assignment(){
  for(int i = 0; i< n_rows; i++){
    fprintf(stderr,"Node %d belongs to process %d \n",i,node_ownership[i]);
  }
}
void update_mst(Edge edge){
    trees[node_location[edge.node_a]].insert(trees[node_location[edge.node_a]].end(), trees[node_location[edge.node_b]].begin(), trees[node_location[edge.node_b]].end());
    
    trees[node_location[edge.node_b]].clear();

    trees[node_location[edge.node_a]].push_back(edge);

}
void merge(Edge edge){

    if(graph[node_location[edge.node_b]].empty()){
      // fprintf(stderr, "Link to empty! \n");
      //CHECK THIS
      node_location[edge.node_b] = node_location[edge.node_a];
      return;
    }
    // fprintf(stderr, "In MST %d - %d \n",edge.node_a, edge.node_b);
    //Dangerous optimazation!!!!!!!!!!!
    // if(graph[node_a].size() < graph[node_b].size()){
    //   fprintf(stderr, "SWAPP");
    //   swap(graph[node_a], graph[node_b]);
    // }
    
    while(!graph[node_location[edge.node_b]].empty()){
        // fprintf(stderr,"IN MERGE size %d \n",int(graph[node_location[node_b]].size()));
        // fprintf(stderr,"node_a %d node_B %d location_node_a %d location_node_b %d \n", node_a, node_b, node_location[node_a], node_location[node_b]);
        graph[node_location[edge.node_a]].push(graph[node_location[edge.node_b]].top());
        graph[node_location[edge.node_b]].pop();
        // fprintf(stderr,"IN MERGE size %d \n",int(graph[node_location[node_b]].size()));
    }

    update_mst(edge);
    node_location[node_location[edge.node_b]] = node_location[edge.node_a];
    node_location[edge.node_b] = node_location[edge.node_a];

   
    // if(node_a == 5 && node_b == 6){
    //   status_merging();
    //   show_edges(7);
    //   throw;
    // }
  }
bool is_self_edge(int node_a, int node_b){
  return node_location[node_a] == node_location[node_b];
}
bool is_outside_of_process(int process_id, int node){
  // means merging process
  if (process_id == -1){
    return false;
  }
  return node_ownership[node] != process_id;
}
void setup_location_array(){
  for(int i = 0; i < n_rows; i++){
    node_location[i] = i;
  }
}
void boruvka(int process_id){
  fprintf(stderr, "Boruvka for process %d\n",process_id);
 
  for(int i =0; i< n_rows; i++){
    // fprintf(stderr, "Component %d \n",i);
    while(!graph[i].empty()){
      // print_edge(graph[0].top());
      Edge edge_to_merge = graph[i].top();

      if(is_outside_of_process(process_id, edge_to_merge.node_b) ||  is_outside_of_process(process_id, edge_to_merge.node_a)){
        // print_edge(edge_to_merge);
        // fprintf(stderr, "Is outside of process\n");
        break;
      }

      if(is_self_edge(edge_to_merge.node_a,edge_to_merge.node_b)){
        // fprintf(stderr, "self edge\n");
        graph[i].pop();
      }else{
        graph[i].pop();
        merge(edge_to_merge);
        
      };
      
    }
    // fprintf(stderr, "Component %d done \n",i);
    }
    fprintf(stderr, "Total weigth: %f\n", total_weight);
}
void report_results(){
  int number_of_trees = 0;
  double total_weight = 0;
  for(int i = 0; i< n_rows; i++){
    if(!trees[i].empty()){
      double weight = 0;
      fprintf(stderr, "Edges for Tree %d\n",number_of_trees);
      for(int k = 0; k < int(trees[i].size()); k++){
        Edge edge = trees[i][k];
        print_edge(edge);
        weight += edge.weight;
      }
      fprintf(stderr, "Weight for tree %d: %f\n",number_of_trees ,weight);
      number_of_trees +=1;
      total_weight += weight;
    }
  }
  fprintf(stderr, "Weight for all trees %f\n",total_weight);
}
template<typename T>
void printVectorElements(vector<T> &vec)
{
    for (auto i = 0; i < vec.size(); ++i) {
        print_edge(vec.at(i));
    }
    printf("\n");
}
int
main(int argc, char **argv)
{
  if (argc != 2)
    {
      fprintf(stderr, "usage: %s <filename>\n", argv[0]);
      return -1;
    }

  int   numtasks, taskid, len;
  char hostname[MPI_MAX_PROCESSOR_NAME];

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  MPI_Comm_rank(MPI_COMM_WORLD,&taskid);
  MPI_Get_processor_name(hostname, &len);
  printf ("Hello from task %d on %s!\n", taskid, hostname);
  if (taskid == 0){
    printf("MASTER: Number of MPI tasks is: %d\n",numtasks);
    }

  // vector<Edge> i_vec1;
  // vector<Edge> i_vec2;

  // i_vec1.push_back(Edge(1,1,1));
  // i_vec2.push_back(Edge(2,2,2));

  // printVectorElements(i_vec1);
  // i_vec1.insert(i_vec1.end(), i_vec2.begin(), i_vec2.end());
  // printVectorElements(i_vec1);
  // i_vec2.clear();
  // printVectorElements(i_vec2);

  // throw;
  
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
  create_structs();
  fprintf(stderr,"Matrix converted to structs \n");
  // show_lowest_edge();
  // status_update();

  auto start_time = std::chrono::high_resolution_clock::now();
  
  setup_location_array();
  // show_edges(5);
  divide_nodes(0,numtasks);
  // show_node_assignment();
  boruvka(taskid);
  // int number;
  if(taskid != 0){
    // number = 42;
    // MPI_Send(&number, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  }

  if(taskid == 0){
    report_results();
    // MPI_Recv(&number, 1, MPI_INT, 1, 0, MPI_COMM_WORLD,
    //          MPI_STATUS_IGNORE);
    // printf("Process 1 received number %d from process 0\n",
    //        number);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    fprintf(stdout, "%.20f\n", elapsed_time.count());
  }

  return 0;
}
