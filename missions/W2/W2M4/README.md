## 구현 내용

- `tasks_to_accomplish` Queue에 10개의 작업을 저장한다.

- 4개의 Process를 생성한다.

- 각 Process는 Queue에서 작업을 하나씩 가져와 실행한다.

- 작업 실행 시간은 `time.sleep(0.5)`로 시뮬레이션한다.

- 완료된 작업 메시지는 `tasks_that_are_done` Queue에 저장한다.

- 모든 Process가 종료될 때까지 `join()`으로 기다린다.

- 마지막에 완료 메시지를 출력한다.

## 핵심 개념

이번 과제에서는 작업 Queue와 결과 Queue를 분리했다.

- `tasks_to_accomplish`: 아직 처리되지 않은 작업 저장

- `tasks_that_are_done`: 처리 완료된 결과 저장

각 Process는 `get_nowait()`을 사용해 작업을 가져온다. Queue가 비어 있으면 `queue.Empty` 예외가 발생하며, 이때 반복문을 종료한다.

## 실행 결과가 매번 달라지는 이유

멀티프로세싱 환경에서는 여러 Process가 동시에 Queue에 접근한다.  

따라서 어떤 Process가 어떤 Task를 처리하는지는 실행할 때마다 달라질 수 있다.

즉, 결과에서 중요한 것은 특정 Process가 특정 Task를 처리하는 것이 아니라, 10개의 Task가 모두 처리되고 완료 메시지가 출력되는 것이다.