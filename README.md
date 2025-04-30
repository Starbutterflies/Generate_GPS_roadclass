# Generate_GPS_roadclass  
---  
## 代码总介绍  
实现的功能非常简单，就是给出一个84坐标系的**GPS文件**，返回每一个点对应的**道路信息**。  
**原始数据**：  
![image](https://github.com/user-attachments/assets/928fd47e-6f67-4075-9c84-0c4a4e293b1b)  
**处理后的数据**：  
![image](https://github.com/user-attachments/assets/d33510a4-91e0-4439-97a1-862a40568eeb)  
虽然功能简单，而且很有可能有一些我并不知道的技术路线（比如Arcgis pro/ arcgis等），可以更好的解决这样的问题。但是这是我在研一时的一次尝试，所以我还是打算把它上传到github上。  
## 核心思路  
![image](https://github.com/user-attachments/assets/8166af2b-cb26-49c5-991e-d054f08a2771)  
## 主要模块：  
### 1.utils.py  
  模块里主要是一些工具函数。  
  #### 主要函数  
  - **`judge_begin_end`**  
  主要是用来处理数据。将数据切割成速度首尾都是0的数据。  
  **输入:**  
    data：需要进行判定的原始数据  
  **输出:**  
    data：如果data首尾速度都是0，那么就不进行任何处理。否则，那就向里面搜索速度为0的第一个值，并将那些不为0的值删去。  
    
  - **`wgs84_to_gcj02`**  
  用来将wgs84转化为gcj02的函数，基于pyproj包，因为速度太慢已经弃用  
  **输入:**  
    latitude：维度84  
    longitude：经度84  
  **输出:**  
    latitude：维度02  
    longitude：经度02  

  - **`wgs84togcj02、transformlat、transformlng`**  
  用来将wgs84转化为gcj02的函数，基于一些计算，破解了加密算法，比调包要更快。  
  **输入:**  
    latitude：维度84  
    longitude：经度84  
  **输出:**  
    latitude：维度02  
    longitude：经度02

### 2.main.py  
  模块里是用来切割数据的类。  
  #### 主要类  
  - **`trouble_dealer`**  
  用来将抽取点获取地理位置  
  ##### 主要方法  
  - **read_data**  
  单纯的用于读取数据。并合适有没有将数据完全读进去。如果完全读进去了，就self.bool = True,否则就为False。  
  - **rename_data**  
  处理时间戳数据。因为部分数据是202010071201这样的形式，而有一些却是2022-12-16 17:39:11.000这样的形式因此需要处理为统一的格式。这里只保留了一天的时间，用于判断时间戳有没有断裂。  
  - **judge_deal_data**  
    ①用于看数据是不是以0开头。是不是以0结尾  
    ②将Longitude和Latitude结尾的E和N去掉  
    ③检查时间戳是否连续和完整  
  - **split_interpolate_data**  
  按照时间戳切割数据。将大段的时间切成连续的小段。  
  - **save_data**
  将切割后的数据保存。

### 3.get_position.py  
  模块中是用来获取位置的类。  
  #### 主要类  
  - **`generate_position`**  
  用来生成位置。使用需要提供你的高德API key。获取网站：https://lbs.amap.com/  
  ##### 主要方法  
  - **`init`**  
  将切割后的数据保存。  
  path => 用来添加位置的数据路径。  
  total_point => 取点的数量。注意！total_point//16的意思是用来给不同cpu的分配数据的！  
  - **`request_data`**  
  用来向api请求数据，是多进程请求数据的。  
  df => 用来请求数据的原始df  
  i => 保存文件的序号  
  以下是一个保存数据的示例：  
  ![image](https://github.com/user-attachments/assets/e1bbf736-f3c8-46c9-b56f-8d71b44411ef)
  - **`job_lib_run`**  
  多进程运行

### 4.deal_data.py  
  模块中主要是用来处理位置信息数据的函数。  
  - **`read_position_data`**  
  输入 => 位置信息的路径  
  输出 => 读取后的数据，未填补空值。

  - **`read_origin_data`**  
  输入：原始数据路径  
  输出：拼接好的df  
  - **`align_point`**  
  输入：  
  total_df => 原始的df  
  total_point => 切分的点数，用来和上文中的那个玩意配合。  
  position_data => 获取到的位置信息df  
  输出：  
  对齐后的，包括周围道路信息的df  
  - **`fill_na`**  
  这是一个矫正函数，用来看前后是不是一样的信息。  
  输入：merged_df => 已经拼接好的一个df。  
  如果前后道路名字是一样的，那么就将其间的道路信息填充为前后一样的名字。  
  - **`fill`**  
  这个是一个填补函数，将第一级和第二级数据的信息相互填补。  
  - **`generate_new_position`**  
  再次调用api，访问前后名字不一样数据的所有点。  
  - **`fill_empty`**  
  参数:  
  fill_na_df => 需填补的数据  
  param info_df => 新的位置数据  
  返回：  
  终极的填补数据  
### 5.split_and_classify_data.py  
  这个模块是一个施工到一半的模块，很多功能没有良好的建立和实现。它的作用是将不同的驾驶分割为不同道路的运行：  
  - **`classify_data`**  
  函数作用：将不同种类的数据添加到df中  
  total_df: 实际上应该是total_info_df这里是拼写错误。是读取的原始数据info_df  
  real_df: 道路信息。就是完成填补之后数据的内容。

  - **`split_and_classify_data`**  
  函数作用：切割数据，将数据按照cpu数等量切割成几份。  
  real_road_path: 数据的路径  
  - **`concat_new_position`**  
  结合两个total_info_csv，来自不同的osm数据。  
  - **`return_to_origin`**  
  重新切割后的，连续时序的，并且分好类的数据集，并将数据保存在文件中。  
  path => 读取数据的路径  

  - **`time_split`**  
    这是未完成的功能。
