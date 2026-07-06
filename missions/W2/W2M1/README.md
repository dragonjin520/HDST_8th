# W1M2

## 시작 전에 생각해볼것

- Pool을 쓰는 이유?
    같은 형태의 독립적인 작업이 여러 개일때 유리.
    실행 단위가 process이기 때문에 별도의 메모리를 사용하여 독립적인 작업에 유리하다.
    또한 네트워크 요청을 보내는 작업이 아니라 CPU 계산을 시뮬레이션하는 작업이므로 Pool을 쓰는게 적합하다.

- worker가 2개가 아니라 10개, 100개라면?
    multiprocessing.Pool의 성능은 CPU core 수와 관련이 있다. 
    따라서 CPU core의 수를 넘어가는 worker의 수는 오히려 비효율적이다.


- 중간에 실패하면 다시 시작할 수 있는가?
    worklog 함수를 사용해서 실패까지 기록하게 만든다.


## 작업 순서

1. config.py 작성

2. src/pool_example.py 작성

3. output/task_results.csv 저장 기능 추가

5. w2m1.ipynb 작성



## 확장 구현: Worker Timeline 기록

기본 요구사항은 각 작업의 시작과 종료 메시지를 출력하는 것이다. 하지만 작업 수가 많아질 경우 터미널 출력만으로는 어떤 worker가 어떤 작업을 처리했는지 파악하기 어렵다.

따라서 각 작업의 시작 시점, 종료 시점, 처리한 프로세스 ID, worker label을 기록하여 `task_results.csv`로 저장하였다. 또한 전체 실행 시간 기준으로 각 worker가 어떤 시점에 어떤 작업을 실행 중이었는지 확인할 수 있도록 `task_timeline.csv`를 생성하였다.

이를 통해 단순히 작업이 완료되었는지뿐만 아니라, worker pool이 작업을 어떻게 분배했는지 시간축 기준으로 확인할 수 있다.