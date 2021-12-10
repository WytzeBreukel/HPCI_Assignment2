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
    Edge(int node_a = 0, int node_b = 0, double weight =0.0)
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
const int max_n_elements = 92493820;
// const int max_n_elements = 131072;
// const int max_n_elements = 760648352;
// const int max_n_rows = 16384;
// const int max_n_rows =  27993600;
              
const int max_n_rows =	999205;
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
MPI_Datatype mpi_edge_type;




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
void snapshot(){
  for(int i = 0; i < n_rows; i++){
    fprintf(stderr,"ID = %d size = %d \n",i,int(graph[i].size()));
    if(graph[i].size() != 0){
      print_edge(graph[i].top());
    }
    fprintf(stderr, "\n");
  }
}

void snapshot_trees(){
  for(int i = 0; i < n_rows; i++){
    fprintf(stderr,"ID = %d size = %d \n",i,int(trees[i].size()));
    if(trees[i].size() != 0){
      print_edge(trees[i][0]);
    }
    fprintf(stderr, "\n");
  }
}

void hash_trees(){
  int size =0;
  for(int i = 0; i < n_rows; i++){
    size += int(trees[i].size());
  }
  fprintf(stderr, "SIZE OF All treees %d\n", size);
}

void hash_edges(){
  int size =0;
  for(int i = 0; i < n_rows; i++){
    size += int(graph[i].size());
  }
  fprintf(stderr, "SIZE OF All ownership %d\n", size);
}

void hash_ownership(){
  int size =0;
  for(int i = 0; i < n_rows; i++){
    size += node_ownership[i];
  }
  fprintf(stderr, "SIZE OF All edges %d\n", size);
}

double retrive_value(int row, int column){
   for(int idx = row_ptr_begin[row]; idx <= row_ptr_end[row]; idx++){
    if(column == col_ind[idx]){
      return values[idx];
    }
  }
  return 0; 
}

void divide_nodes(int amount_of_process){
  int process_id = 0;
  int amount_of_nodes_per_process = n_rows/amount_of_process;
  // throw;

  int nodes_assigned = 0;



  for(int i = 0; i < n_rows; i++){
  int starting_node = i;
  if(!visited_nodes[starting_node]){
    visited_nodes[starting_node] = true;
    // fprintf(stderr," %d",starting_node);

    to_visit.push(starting_node);
    while(!to_visit.empty()){
      for(int idx = row_ptr_begin[starting_node]; idx<= row_ptr_end[starting_node]; idx++){
            if(!visited_nodes[col_ind[idx]]){
              // fprintf(stderr," %d",col_ind[idx]);  
              nodes_assigned = nodes_assigned + 1;
              if(nodes_assigned >= amount_of_nodes_per_process){
                process_id = process_id + 1;
                nodes_assigned = 0;
              }
              to_visit.push(col_ind[idx]);
              visited_nodes[col_ind[idx]] = true;


              node_ownership[col_ind[idx]] = process_id;
              
            
            }
      }
      // fprintf(stderr,"%d \n",starting_node);
      to_visit.pop();
      starting_node = to_visit.front();
      // fprintf(stderr,"\n");
    }
  }
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

    total_weight += edge.weight;
    

}
void merge(Edge edge){

    // if(graph[node_location[edge.node_b]].empty()){
    //   // fprintf(stderr, "Link to empty! \n");
    //   //CHECK THIS
    //   node_location[edge.node_b] = node_location[edge.node_a];
    //   return;
    // }
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

    int temp = node_location[edge.node_b ];
    for(int i = 0; i < n_rows;i++)
    {
        if(node_location[i] == temp){
        node_location[ i ] = node_location[edge.node_a]; 
      }
    }

   
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
    // fprintf(stderr, "Total weigth: %f\n", total_weight); 
}
void report_results(){
  int number_of_trees = 0;
  double total_weight = 0;
  for(int i = 0; i< n_rows; i++){
    // fprintf(stderr,"REPORTIN results for %d \n",i);
    if(!trees[i].empty()){
      double weight = 0;
      // fprintf(stderr, "Edges for Tree %d\n",number_of_trees);
      for(int k = 0; k < int(trees[i].size()); k++){
        Edge edge = trees[i][k];
        // print_edge(edge);
        weight += edge.weight;
      }
      // fprintf(stderr, "Weight for tree %d: %f\n",number_of_trees ,weight);
      number_of_trees +=1;
      total_weight += weight;
    }
  }
  // fprintf(stderr, "There are %d trees with no edges making then total amount of trees: %d\n", nodes_with_no_edges, number_of_trees+nodes_with_no_edges);
    fprintf(stderr, "Total amount of trees: %d\n", number_of_trees);
  fprintf(stderr, "Weight for all trees %f\n",total_weight);
}

int find_nodes_with_no_edges(){
  int nodes_with_no_edges = 0;
  for(int i = 0; i < n_rows; i++){
    if(graph[i].empty()){
      printf("EMPTY!");
      nodes_with_no_edges +=1;
    }
  }
  printf("No edge %d\n", nodes_with_no_edges);
  return nodes_with_no_edges;
}
void send_trees(int task_id){
  vector<int> ids;
  vector<int> sizes;
  for(int i =0; i< n_rows; i++){
    if(node_ownership[i] == task_id){
      ids.push_back(i);
      sizes.push_back(trees[i].size());
    }
  }

  int amount_of_trees = ids.size();

  printf("AMOUNT OF trees before sending %d \n",amount_of_trees);
  MPI_Send(&amount_of_trees, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  for(int i = 0; i<amount_of_trees; i++){
    int information[2] = {ids[i], sizes[i]};
    // printf("SENDING TREE ID %d size %d \n", ids[i],sizes[i]);
    MPI_Send(&information, 2, MPI_INT, 0, 0, MPI_COMM_WORLD);
    // printf("COMPLETED SENDING TREE ID %d size %d \n", ids[i],sizes[i]);
    if(sizes[i] != 0){
        vector<Edge> trees_to_send = trees[information[0]];
      
        // for(int i = 0; i< trees_to_send.size(); i++){
        //   print_edge(trees_to_send[i]);
        // }
        MPI_Send(&trees_to_send[0],sizes[i], mpi_edge_type,0, 0, MPI_COMM_WORLD);
       
    }
    
  }

}

void send_edges(int task_id){
  vector<int> ids;
  vector<int> sizes;
  for(int i =0; i< n_rows; i++){
    if(node_ownership[i] == task_id){
      ids.push_back(i);
      sizes.push_back(graph[i].size());
    }
  }

  int amount_of_components = ids.size();

  printf("AMOUNT OF compents before sending %d \n",amount_of_components);
  MPI_Send(&amount_of_components, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  for(int i = 0; i<amount_of_components; i++){
    int information[2] = {ids[i], sizes[i]};
    // printf("ID %d size %d \n", ids[i],sizes[i]);
    MPI_Send(&information, 2, MPI_INT, 0, 0, MPI_COMM_WORLD);
    if(sizes[i] != 0){
        vector<Edge> edges;
        while(!graph[information[0]].empty()){
          edges.push_back(graph[information[0]].top());
          graph[information[0]].pop();
        }
        // printf("Size %d\n",sizes[i]);

      //   for(int k = 0; k< sizes[i]; k++){
      //   printf("TEST vector pre send %d %d %f\n",edges[k].node_a,edges[k].node_b,edges[k].weight);
      // }
        
      MPI_Send(&edges[0],sizes[i], mpi_edge_type,0, 0, MPI_COMM_WORLD);
       
    }
  }

}

void recieve_edges(int task_id){
  printf("Recieveing components from %d\n", task_id);
  int amount_of_components;
  MPI_Recv(&amount_of_components, 1, MPI_INT, task_id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  printf("amount of components from %d: %d\n",task_id, amount_of_components);
  for(int i = 0; i<amount_of_components; i++){
    int information[2];
    MPI_Recv(&information, 2, MPI_INT, task_id, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
  
    // printf("INFO %d %d \n", information[0],information[1]);

    if(information[1] == 0 ){
      graph[information[0]] = priority_queue<Edge, vector<Edge>, CompareWeight>();
    }else{
      vector<Edge> edges;
      edges.resize(information[1]);

      MPI_Recv(&edges[0], information[1], mpi_edge_type, task_id, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      // for(int i = 0; i< information[1]; i++){
      //   printf("TEST vector post send %d %d %f\n",edges[i].node_a,edges[i].node_b,edges[i].weight);
      // }
      graph[information[0]] = priority_queue<Edge, vector<Edge>, CompareWeight>();

      for(int k = 0; k < information[1]; k++){
        graph[information[0]].push(edges[k]);
      }
    
  }
  }
}

void recieve_trees(int task_id){
  printf("Recieveing trees from %d\n", task_id);
  int amount_of_trees;
  MPI_Recv(&amount_of_trees, 1, MPI_INT, task_id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  printf("amount of trees from %d: %d\n",task_id, amount_of_trees);
  // printf("amount of components %d\n",amount_of_trees);
  for(int i = 1; i<amount_of_trees; i++){
    int information[2];
    // printf("RECIVEDing TREE  INFO for %d  \n",i);
    MPI_Recv(&information, 2, MPI_INT, task_id, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
  
    // printf("RECIVED INFO %d %d \n", information[0],information[1]);

    if(information[1] == 0 ){
      // fprintf(stderr, "ZEROO\n");
      if(!trees[information[0]].empty()){
        fprintf(stderr, "WIERDDDD");
        throw;
      }
      trees[information[0]] = vector<Edge>();
    }else{
      // trees[information[0]] = vector<Edge>();
      vector<Edge> recived_trees;
      recived_trees.resize(information[1]);

      MPI_Recv(&recived_trees[0], information[1], mpi_edge_type, task_id, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      // for(int i = 0; i< information[1]; i++){
      //   printf("TEST vector post send %d %d %f\n",edges[i].node_a,edges[i].node_b,edges[i].weight);
      // }
      trees[information[0]] = recived_trees;

    }
    
  }
}
void merge_location_arrays(int received_location_array[]){
  for(int i = 0; i < n_rows; i++){
    if(received_location_array[i] != i){
      //sanity check 
      if(node_location[i] != i){
        printf("THIS SHOULD NOT HAPPEN\n");
        throw;
      }
      node_location[i] = received_location_array[i];
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
  int   numtasks, taskid, len;
  char hostname[MPI_MAX_PROCESSOR_NAME];

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

  const int nitems=3;
  int          blocklengths[3] = {1,1,1};
  MPI_Datatype types[3] = {MPI_INT, MPI_INT,MPI_DOUBLE};
 
  MPI_Aint     offsets[3];

  offsets[0] = offsetof(Edge, node_a);
  offsets[1] = offsetof(Edge, node_b);
  offsets[2] = offsetof(Edge, weight);

  MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_edge_type);
  MPI_Type_commit(&mpi_edge_type);
  MPI_Comm_rank(MPI_COMM_WORLD,&taskid);
  MPI_Get_processor_name(hostname, &len);
  printf ("Hello from task %d on %s!\n", taskid, hostname);
  if (taskid == 0){
    printf("MASTER: Number of MPI tasks is: %d\n",numtasks);
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
  auto start_time = std::chrono::high_resolution_clock::now();
  create_structs();
  fprintf(stderr,"Matrix converted to structs \n");
  // int nodes_with_no_edges = 0;
  // if(taskid == 0){
  //   nodes_with_no_edges = find_nodes_with_no_edges();
  // }

  auto create_structs_time = std::chrono::high_resolution_clock::now();
  
  setup_location_array();
  if(numtasks == 1){
    
    boruvka(-1);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    std::chrono::duration<double> create_structs_elapsed_time = create_structs_time - start_time;
    std::chrono::duration<double> create_boruvka_elapsed_time = end_time - create_structs_time;
    report_results();
    fprintf(stdout, "Struct Time  %.20f\n", create_structs_elapsed_time.count());
    fprintf(stdout, "boruvka_time  %.20f\n", create_boruvka_elapsed_time.count());
    fprintf(stdout, "%.20f\n", elapsed_time.count());
    return 0;
  }
 
  divide_nodes(numtasks);

  auto divide_nodes_time = std::chrono::high_resolution_clock::now();
  // show_node_assignment();
  // boruvka(0);
  // boruvka(1);
  
  // // status_merging();
  
  // boruvka(-1);
  // fprintf(stderr, "TOTAL WEIGHT %f\n", total_weight);
  // report_results(nodes_with_no_edges);
  // return 0;
  
  boruvka(taskid);
  if(taskid != 0){

    MPI_Send(&node_location, n_rows, MPI_INT, 0, 0, MPI_COMM_WORLD);

    send_edges(taskid);
    send_trees(taskid);
    

    MPI_Finalize();
    
  }

  if(taskid == 0){
    
    for(int i = 1; i< numtasks; i++){
      int received_location_array[max_n_rows];
      MPI_Recv (&received_location_array,n_rows,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);

      merge_location_arrays(received_location_array);
    }
    // status_merging();
    for(int i= 1; i< numtasks; i++){
      recieve_edges(i);
      recieve_trees(i);
    }

    // snapshot();
    //  snapshot_trees();

    fprintf(stderr, "VOOR borouvka\n ");
    // hash_ownership();
    // throw;
    boruvka(-1);
    fprintf(stderr,"Done merging \n");
    // report_results();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    std::chrono::duration<double> struct_time_time_elapsed = create_structs_time - start_time;
    std::chrono::duration<double> divide_nodes_time_elapsed = divide_nodes_time - create_structs_time;
    std::chrono::duration<double> boruvka_time = end_time - divide_nodes_time;

    fprintf(stdout, "Struct Time  %.20f\n", struct_time_time_elapsed.count());
    fprintf(stdout, "Deivide_nodes  %.20f\n", divide_nodes_time_elapsed.count());
    fprintf(stdout, "boruvka_time  %.20f\n", boruvka_time.count());
    fprintf(stdout, " Total time %.20f\n", elapsed_time.count());


    MPI_Finalize();
  }

  return 0;
}
