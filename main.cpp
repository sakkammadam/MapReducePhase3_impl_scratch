#include <iostream>
#include <thread>
#include <future>
#include <mutex>
#include <filesystem>
#include <dlfcn.h>
#include <algorithm>
#include "FileProcessorBase.hpp"
#include "MapperBase.hpp"
#include "ShufflerBase.hpp"
#include "ReducerBase.hpp"

// Mutex for mapper operations
std::mutex mapper_mutex;
std::mutex mapper_ind_mutex;
std::mutex mapper_op_mutex;
std::mutex mapper_ld_mutex;
std::mutex mapper_fp_mutex;

// Mutex for shuffler operations
std::mutex shuffler_mutex;
std::mutex shuffler_ind_mutex;
std::mutex shuffler_op_mutex;
std::mutex shuffler_ld_mutex;
std::mutex shuffler_fp_mutex;

// Mutex for reducer operations
std::mutex reducer_mutex;
std::mutex reducer_ind_mutex;
std::mutex reducer_op_mutex;
std::mutex reducer_ld_mutex;
std::mutex reducer_fp_mutex;

void func(std::promise<int> &&p){
    p.set_value(1);
}

int evalFolders(std::string root_directory){
    // declare a vector that will hold the all mapper folders!
    std::vector<std::string> mapper_folders;
    // iterate and load directory_files vector!
    for(const auto &entry:std::filesystem::directory_iterator(root_directory)){
        mapper_folders.push_back(entry.path());
    }
    return mapper_folders.size();
}

auto fileProcessInputs(FileProcessorBase* obj){
    obj->runOperation();
    return obj->getInputDirectoryData();
}

auto mapperOps(MapperBase* obj){
    mapper_ind_mutex.lock();
    obj->runMapOperation();
    mapper_ind_mutex.unlock();
    return obj->getMapperOutput();
}

auto fileProcessMapOutputs(FileProcessorBase* obj){
    mapper_fp_mutex.lock();
    obj->runOperation();
    mapper_fp_mutex.unlock();
    return obj->getMapperOutputDirectory();
}

auto shufflerOps(ShufflerBase* obj){
    shuffler_ind_mutex.lock();
    obj->runShuffleOperation();
    shuffler_ind_mutex.unlock();
    return obj->getShuffledOutput();
}

auto fileProcessShufOutputs(FileProcessorBase* obj){
    shuffler_fp_mutex.lock();
    obj->runOperation();
    shuffler_fp_mutex.unlock();
    return obj->getShufflerOutputDirectory();
}

auto reducerOps(ReducerBase* obj){
    reducer_ind_mutex.lock();
    obj->runReduceOperations();
    reducer_ind_mutex.unlock();
    return obj->getReducedOutput();
}

auto fileProcessRedOutputs(FileProcessorBase* obj){
    reducer_fp_mutex.lock();
    obj->runOperation();
    reducer_fp_mutex.unlock();
    return obj->getFinalOutputDirectory();
}

int main() {
    std::cout << "Hello, World!" << std::endl;
    std::promise<int> prom;
    auto f = prom.get_future();
    std::thread t(func, std::move(prom));
    t.join();
    int i_x = f.get();
    std::cout << "Value from future! << " << i_x << std::endl;

    // load the FileProcessor library
    void* fileProcessorLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/FileProcessorInput.so",RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!fileProcessorLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // reset errors
    dlerror();
    // load the symbols associated with function pointer that will create new instance object of FileProcessor
    // it will call the constructor specifically for input operations!
    create_t* create_InputDirectoryFP_Obj = (create_t*)dlsym(fileProcessorLib,"createInputObj");
    const char* dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from FileProcessor lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated with function pointer that will delete the instance object
    destroy_t* destroy_InputDirectoryFP_Obj = (destroy_t*)dlsym(fileProcessorLib,"removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from FileProcessor lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the Mapper library
    void* mapperLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/MapperImpl.so",RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!mapperLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // reset errors
    dlerror();
    // load the symbols associated with function pointer that will create new instance of Mapper
    // it will call constructor specifically for mapper operations!
    createMapper_t* create_Mapper_Obj = (createMapper_t*)dlsym(mapperLib,"createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from Mapper lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated with function pointer that will delete the instance object
    destroyMapper_t* destroy_Mapper_Obj = (destroyMapper_t*)dlsym(mapperLib,"removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from Mapper lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // reset errors
    dlerror();
    // load the FileProcessorMapOutput library
    void* fpMapperOpLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/FileProcessorMapOutput.so", RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!fpMapperOpLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // load the symbols associated with function pointer that will create new instance of FileProcessorMapOutput
    readMapperOp_t* create_MapperFP_Obj = (readMapperOp_t*)dlsym(fpMapperOpLib, "createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpMapperOpLib lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated with function pointer that will delete the instance object
    destroyMapperOp_t* destroy_MapperFP_Obj = (destroyMapperOp_t*)dlsym(fpMapperOpLib, "removeInputObj");
    dlsym_error = dlerror();
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpMapperOpLib lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // reset errors
    dlerror();
    // load the shuffler library
    void* shufflerLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/ShufflerImpl.so", RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!shufflerLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // load the symbols associated with function pointer that will create new instance of Shuffler
    createShuffler_t* create_Shuffler_Obj = (createShuffler_t*)dlsym(shufflerLib, "createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from shufflerLib lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated with the function pointer that will delete the instance object
    destroyShuffler_t* destroy_Shuffler_Obj = (destroyShuffler_t*)dlsym(shufflerLib, "removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from shufflerLib lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the FileProcessorShufOutput library
    void* fpShufflerLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/FileProcessorShufOutput.so", RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!fpShufflerLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // load the symbols associated with function pointer that will create new instance of FileProcessorShufOutput
    readShufflerOp_t* create_ShufflerFP_Obj = (readShufflerOp_t*)dlsym(fpShufflerLib,"createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpShufflerLib lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated the function pointer that destroy instance of FileProcessorShufOutput
    destroyShufflerOp_t* destroy_ShufflerFP_Obj = (destroyShufflerOp_t*)dlsym(fpShufflerLib,"removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpShufflerLib lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // reset errors
    dlerror();
    // load the reducer library
    void* reducerLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/ReducerImpl.so", RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!reducerLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // load the symbols associated with function pointer that will create new instance of Reducer
    createReducer_t* create_Reducer_Obj = (createReducer_t*)dlsym(reducerLib, "createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from reducerLib lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated with the function pointer that will delete the instance object
    destroyReducer_t* destroy_Reducer_Obj = (destroyReducer_t*)dlsym(reducerLib, "removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from reducerLib lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the FileProcessorRedOutput library
    void* fpReducerLib = dlopen("/home/sakkammadam/myLocalLibs/mapReduce/FileProcessorRedOutput.so", RTLD_LAZY);
    // raise error if the library wasn't loaded
    if(!fpReducerLib){
        std::cerr << "Cannot load library: " << dlerror() << std::endl;
        return 1;
    }
    // load the symbols associated with function pointer that will create new instance of FileProcessorRedOutput
    readReducerOp_t* create_ReducerFP_Obj = (readReducerOp_t*)dlsym(fpReducerLib,"createInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpReducerLib lib createInputObj: " << dlsym_error << std::endl;
        return 1;
    }
    // load the symbol associated the function pointer that destroy instance of FileProcessorShufOutput
    destroyReducerOp_t* destroy_ReducerFP_Obj = (destroyReducerOp_t*)dlsym(fpReducerLib,"removeInputObj");
    dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        std::cerr << "Cannot load symbol from fpReducerLib lib removeInputObj: " << dlsym_error << std::endl;
        return 1;
    }


    // NEW!
    std::string input_directory = "/home/sakkammadam/Documents/syr/cse687/projects/input_files/shakeys/";
    // declare a vector that will hold the all files in a directory!
    std::vector<std::string> directory_files;
    // iterate and load directory_files vector!
    for(const auto &entry:std::filesystem::directory_iterator(input_directory)){
        if(std::filesystem::is_regular_file(entry)){
            directory_files.push_back(entry.path());
        }
    }
    // declare a vector that will hold all fileProcessorInput objects
    std::vector<FileProcessorBase*> fp_objects;
    // use directory_files vector to load fp_objects vector
    for(const auto &file: directory_files){
        fp_objects.push_back(create_InputDirectoryFP_Obj("input",file));
    }
    // declare a vector of futures that will host results of file processor input operations
    std::vector<std::future<std::map<std::string, std::vector<std::vector<std::string>>>>> load_dir_files;
    // use fp_objects vector to call individual objects and load the load_dir_files vector
    for(auto obj: fp_objects){
        load_dir_files.push_back(std::async(fileProcessInputs,obj));
    }
    std::cout << "There are " << load_dir_files.size() << " future objects in load_dir_files..." << std::endl;

    // declare a vector that will hold all mapper objects
    std::vector<MapperBase*> mapper_objects;

    // iterate over the load_dir_files vector...
    for(auto i=0; i < load_dir_files.size(); i++){
        const std::map<std::string, std::vector<std::vector<std::string>>> &retInput = load_dir_files[i].get();
        // displaying data!
        for(const auto& row: retInput){
            std::cout << row.first << std::endl;
            for(int _i=0; _i < row.second.size(); _i++){
                std::cout << "Operating on file - " << row.first << std::endl;
                std::cout << "Working on partition#" << _i << std::endl;
                // create temp obj
                std::map<std::string, std::vector<std::string>> tempObj;
                tempObj.insert({row.first,row.second[_i]});
                std::cout << "Creating Mapper#" << _i << std::endl;
                mapper_objects.push_back(create_Mapper_Obj(_i, tempObj));
            }
        }
    }
    // declare a vector of futures that will host results of mapper operations
    std::vector<std::future<std::map<std::string, std::vector<std::vector<std::tuple<std::string, int, int>>>>>> mapped_data;

    // use mapper_objects vector to call individual objects and load the mapper_data vector
    std::cout << "There are " << mapper_objects.size() << " mappers" << std::endl;

    // run the mapper operations using mappers!
    for(auto obj:mapper_objects){
        mapper_mutex.lock();
        mapped_data.push_back(std::async(mapperOps,obj));
        mapper_mutex.unlock();
    }

    std::cout << "There are " << mapped_data.size() << " future objects in mapper_data vector...." << std::endl;


    // declare a vector that will hold all fileProcessorMapOutput objects
    std::vector<FileProcessorBase*> fp_map_outputs;

    // load the mapper output to disk using FileProcessorMapOutput
    for(auto i=0; i < mapped_data.size(); i++){
        mapper_op_mutex.lock();
        const std::map<std::string, std::vector<std::vector<std::tuple<std::string, int, int>>>> &retInput = mapped_data[i].get();
        // supply retInput as arguments to FileProcessorMapOutput
        fp_map_outputs.push_back(create_MapperFP_Obj("mapper",retInput));
        mapper_op_mutex.unlock();
    }

    // declare a vector of futures that will host results of mapper file processor output operations
    std::vector<std::future<std::string>> fp_map_output_dirs;

    std::mutex hope;

    hope.lock();
    // run the file processor mapper output operation...
    for(auto obj: fp_map_outputs){
        mapper_ld_mutex.lock();
        fp_map_output_dirs.push_back(std::async(fileProcessMapOutputs,obj));
        mapper_ld_mutex.unlock();
    }
    hope.unlock();

    std::string mapper_root_directory = fp_map_output_dirs.front().get();

    // original file count
    int og_file_count = directory_files.size();
    int current_mapper_count = evalFolders(mapper_root_directory);

    std::cout << "Original file count " << og_file_count << std::endl;
    std::cout << "Current mapper count " << current_mapper_count << std::endl;

    while(og_file_count != current_mapper_count){
        current_mapper_count = evalFolders(mapper_root_directory);
    }

    std::cout << "All mapper output has been written to this root directory - " << mapper_root_directory << std::endl;


    // declare a vector that will hold the all mapper folders!
    std::vector<std::string> mapper_folders;
    // iterate and load directory_files vector!
    for(const auto &entry:std::filesystem::directory_iterator(mapper_root_directory)){
        mapper_folders.push_back(entry.path());
    }
    std::cout << "The individual temp_mapper folders are: " << std::endl;
    for(const std::string &folder:mapper_folders){
        std::cout << folder << std::endl;
    }

    std::cout << "Proceeding to create Shuffler objects to operate against temp_mapper sub-folders..." << std::endl;

    // declare a vector of shuffler objects
    std::vector<ShufflerBase*> shuffler_objects;
    // load the vector of shuffler objects by supplying the individual temp_mapper folders...
    for(const std::string &folder:mapper_folders){
        shuffler_objects.push_back(create_Shuffler_Obj(folder));
    }

    // declare a vector to store future results of shuffler operations
    std::vector<std::future<std::vector<std::map<std::string, std::map<std::string,size_t>>>>> shuffler_data;
    // load the vector with shuffler futures
    for(auto obj:shuffler_objects){
        shuffler_mutex.lock();
        shuffler_data.push_back(std::async(shufflerOps,obj));
        shuffler_mutex.unlock();
    }
    std::cout << "There are " << shuffler_data.size() << " future objects in shuffler_data vector...." << std::endl;

    // declare a vector that will hold all fileProcessorShufOutput objects
    std::vector<FileProcessorBase*> fp_shuf_outputs;
    // pass the shuffler output to FileProcessorShufOutput
    for(auto i=0; i < shuffler_data.size(); i++){
        shuffler_op_mutex.lock();
        const std::vector<std::map<std::string, std::map<std::string,size_t>>> &shufOutput = shuffler_data[i].get();
        shuffler_op_mutex.unlock();
        // supply shufOutput as arguments to FileProcessorShufOutput
        fp_shuf_outputs.push_back(create_ShufflerFP_Obj("shuffler",shufOutput));
    }
    // declare a vector of futures that will host results of shuffler file processor output operations
    std::vector<std::future<std::string>> fp_shuf_output_dirs;
    // run the file processor shuffler output operation....
    // fileProcessShufOutputs will cause the obj to write data to disk and return the directory to fp_shuf_output_dirs
    for(auto obj: fp_shuf_outputs){
        shuffler_ld_mutex.lock();
        fp_shuf_output_dirs.push_back(std::async(fileProcessShufOutputs,obj));
        shuffler_ld_mutex.unlock();
    }
    // Root shuffler directory
    std::string shuffler_root_directory = fp_shuf_output_dirs.front().get();

    // track the current shuffler folder count ...
    int current_shuffler_count = evalFolders(shuffler_root_directory);
    std::cout << "Current shuffler count " << current_shuffler_count << std::endl;

    while(og_file_count != current_shuffler_count){
        current_shuffler_count = evalFolders(shuffler_root_directory);
    }
    bool chk = false;
    while(!chk){
        // Map containing input files in directory1
        std::map<std::string, int> dirShuFiles;
        // Vector containing files that don't exist
        std::vector<std::string> filesDontExist;
        // get a map of all files in mapper directory!
        for(const auto &sub_directory:std::filesystem::directory_iterator(shuffler_root_directory)){
            if(std::filesystem::is_directory(sub_directory)){
                for(const auto &file:std::filesystem::directory_iterator(sub_directory)){
                    std::string shuFileName = file.path().string().substr(file.path().string().rfind('/') + 1);
                    dirShuFiles.insert({shuFileName,1});
                }
            }
        }
        // now we will iterate over the mapper directory!
        for(const auto &sub_directory:std::filesystem::directory_iterator(mapper_root_directory)){
            if(std::filesystem::is_directory(sub_directory)){
                for(const auto &file:std::filesystem::directory_iterator(sub_directory)){
                    std::string mapFileName = file.path().string().substr(file.path().string().rfind('/') + 1);
                    // Let's create an iterator against dirMapFiles and see if shuFileName exists there
                    auto mapItr = dirShuFiles.find(mapFileName);
                    // Check if iterator was exhausted
                    if (mapItr == dirShuFiles.end()) {
                        // if parsedToken was not found! - lets create a entry in filesDontExist vector
                        filesDontExist.push_back(mapFileName);
                    }
                }
            }
        }
        std::cout << "Files dont exist: " << filesDontExist.size() << std::endl;
        // now we will check the filesDontExist vector - if its empty - all shuffling is complete
        if(filesDontExist.empty()){
            // break out of the loop.
            chk=true;
        }
    }

    std::cout << "All Shuffler output has been written to this root directory - " << shuffler_root_directory << std::endl;
    // declare a vector that will hold the all shuffler folders!
    std::vector<std::string> shuffler_folders;
    // iterate and load directory_files vector!
    for(const auto &entry:std::filesystem::directory_iterator(shuffler_root_directory)){
        shuffler_folders.push_back(entry.path());
    }
    std::cout << "The individual temp_shuffler folders are: " << std::endl;
    for(const std::string &folder:shuffler_folders){
        std::cout << folder << std::endl;
    }


    std::cout << "Proceeding to create Reducer objects to operate against temp_shuffler sub-folders..." << std::endl;
    // declare a vector of shuffler objects
    std::vector<ReducerBase*> reducer_objects;
    // load the vector of reducer objects by supplying the individual temp_shuffler folders...
    for(const std::string &folder:shuffler_folders){
        reducer_objects.push_back(create_Reducer_Obj(folder));
    }
    // declare a vector to store future results of reducer operations
    std::vector<std::future<std::map<std::string, std::map<std::string,size_t>>>> reducer_data;
    // load the vector with reducer futures
    for(auto obj:reducer_objects){
        reducer_mutex.lock();
        reducer_data.push_back(std::async(reducerOps,obj));
        reducer_mutex.unlock();
    }
    std::cout << "There are " << reducer_data.size() << " future objects in reducer vector...." << std::endl;
    // declare a vector that will hold all fileProcessorRedOutput objects
    std::vector<FileProcessorBase*> fp_red_outputs;
    // pass the reducer output to FileProcessorRedOutput
    for(auto i=0; i < reducer_data.size(); i++){
        reducer_op_mutex.lock();
        const std::map<std::string, std::map<std::string,size_t>> &redOutput = reducer_data[i].get();
        // supply redOutput as arguments to FileProcessorRedOutput
        fp_red_outputs.push_back(create_ReducerFP_Obj("reducer",redOutput));
        reducer_op_mutex.unlock();
    }
    // declare a vector of futures that will host results of reducer file processor output operations
    std::vector<std::future<std::string>> fp_red_output_dirs;
    // run the file processor reducer output operation....
    // fileProcessRedOutputs will cause the obj to write data to disk and return the directory to fp_red_output_dirs
    for(auto obj: fp_red_outputs){
        reducer_ld_mutex.lock();
        fp_red_output_dirs.push_back(std::async(fileProcessRedOutputs,obj));
        reducer_ld_mutex.unlock();
    }
    // Root shuffler directory
    std::string reducer_root_directory = fp_red_output_dirs.front().get();
    std::cout << "All final output has been written to this root directory - " << reducer_root_directory << std::endl;


    /*

    // destroy the instance
    for(auto obj: fp_objects){
        destroy_InputDirectoryFP_Obj(obj);
    }

    for(auto obj:mapper_objects){
        destroy_Mapper_Obj(obj);
    }

    // unload the handle
    dlclose(fileProcessorLib);
    dlclose(mapperLib);

    */

    return 0;
}
