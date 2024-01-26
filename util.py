import math
import platform
import subprocess

from pathlib import Path


def available_ram_bytes():
    if platform.system() == "Linux":
        free_b = subprocess.run(['free', '-b'], capture_output=True, text=True, check=True)
        free_mem = free_b.stdout.split('\n')[1].split()[3]

        return free_mem

    if platform.system() == "Darwin":
        vm_output = subprocess.run(['vm_stat'], capture_output=True, text=True, check=True)

        pages_free = int(vm_output.stdout.split('Pages free:')[1].lstrip().split('.\n')[0])
        pages_inactive = int(vm_output.stdout.split('Pages inactive:')[1].lstrip().split('.\n')[0])

        result = subprocess.run(['sysctl', 'vm.pagesize'], capture_output=True, text=True, check=True)
        page_size = int(result.stdout.strip().split(': ')[1])

        free_mem = (pages_free + pages_inactive) * page_size

        return free_mem

    if platform.system() == "Windows":
        cmd = ['wmic', 'OS', 'get', 'FreePhysicalMemory']
        free_mem_kb = subprocess.run(cmd, capture_output=True, text=True, check=True)
        free_mem = int(free_mem_kb.stdout.split('\n')[1].strip()) * 1024

        return free_mem

    raise Exception("Unsupported Operating System")


def calculate_group_buckets(source_path, fraction_of_ram=0.25, memory_multiplier=2.5):
    file_size_bytes = Path(source_path).stat().st_size

    available_ram = available_ram_bytes()

    # Estimate the in-memory size of the data, applying a memory multiplier
    estimated_memory_usage = file_size_bytes * memory_multiplier

    max_memory_per_bucket = available_ram * fraction_of_ram

    group_buckets = max(1, math.ceil(estimated_memory_usage / max_memory_per_bucket))

    return group_buckets


def sample_lines_from_file(file_path, sample_size=100):
    lines = []

    with open(file_path, 'r', encoding='utf-8') as file:
        file_size = Path(file_path).stat().st_size

        positions = [file_size * (i // sample_size) for i in range(sample_size)]

        for pos in positions:
            file.seek(pos)
            # Move to the start of the next line if not at the start of a line
            if pos != 0:
                file.readline()

            line = file.readline().strip()
            if line:
                lines.append(line)

            if len(lines) >= sample_size:
                break

    return lines
